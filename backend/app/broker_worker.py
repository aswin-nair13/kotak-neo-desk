"""Single-writer command worker for broker/engine work.

The feed event loop only *enqueues* work here; it never waits for a command
future. One dedicated thread drains the queue in order, so engine transitions
(on_tick / on_order_update / reconcile / route mutations) can never interleave
with each other. Broker HTTP calls happen on this thread too, which is what
keeps a slow/blocked SDK call off the asyncio feed loop.

Two priorities:
  - critical (order updates, reconcile, route mutations) are never dropped;
  - normal (ticks) may be dropped when the queue is full, since a newer tick
    supersedes an older one and ticks must not block the feed reader.

Commands are logged when they take longer than `stall_ms` so a 60 s cancel is
immediately visible instead of looking like a silent feed.
"""

import concurrent.futures
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("worker")


class WorkerQueueFull(RuntimeError):
    """The bounded queue had no room for a caller that needs a result."""


@dataclass
class _Command:
    fn: object
    args: tuple
    kwargs: dict
    label: str
    critical: bool
    order_id: str | None = None
    future: concurrent.futures.Future | None = None
    enqueued_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class CommandWorker:
    def __init__(self, name: str = "engine-worker", max_queue: int = 1000,
                 stall_ms: int = 5000, stop_timeout: float = 5.0,
                 call_timeout: float | None = 10.0):
        self.name = name
        self.max_queue = max(1, int(max_queue))
        self.stall_ms = max(0, int(stall_ms))
        self.stop_timeout = max(0.0, float(stop_timeout))
        self.call_timeout = None if call_timeout is None else max(0.0, float(call_timeout))
        self._critical: deque[_Command] = deque()
        self._normal: deque[_Command] = deque()
        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._last_error: str | None = None
        self._last_label: str | None = None
        self._last_elapsed_ms: int | None = None
        self._last_started_ms: int | None = None
        self._last_finished_ms: int | None = None
        self._completed = 0
        self._failed = 0
        self._dropped = 0
        self._submitted = 0

    # ---------- lifecycle ----------
    def start(self):
        with self._cv:
            if self._thread and self._thread.is_alive():
                # A slow broker call kept the previous worker alive through
                # stop(); let that same thread resume instead of leaking it.
                self._stop.clear()
                self._cv.notify_all()
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name=self.name, daemon=True)
            th = self._thread
            self._cv.notify_all()
        th.start()
        log.info("command worker %s started (queue cap=%d)", self.name, self.max_queue)

    def stop(self, timeout: float | None = None):
        with self._cv:
            self._stop.set()
            pending = list(self._critical) + list(self._normal)
            self._critical.clear()
            self._normal.clear()
            for c in pending:
                if c.future is not None and not c.future.done():
                    c.future.set_exception(RuntimeError("command worker stopped"))
            self._cv.notify_all()
            th = self._thread
        if th and th.is_alive() and th is not threading.current_thread():
            th.join(timeout=self.stop_timeout if timeout is None else timeout)
            if th.is_alive():
                log.warning("command worker %s did not stop within %.1fs "
                            "(a broker call may still be in flight)", self.name,
                            self.stop_timeout if timeout is None else timeout)
        with self._cv:
            if (th is None or not th.is_alive()) and self._thread is th:
                self._thread = None

    def is_running(self) -> bool:
        th = self._thread
        return bool(th and th.is_alive() and not self._stop.is_set())

    # ---------- submission ----------
    def submit(self, fn, *args, critical: bool = False, label: str | None = None,
               order_id: str | None = None, allow_inline: bool = True,
               **kwargs) -> bool:
        """Enqueue without waiting. Returns False when dropped/stopped.

        `allow_inline` keeps unit tests and pre-start callers working: if the
        worker has not been started the call runs on the caller thread. Reconcile
        passes allow_inline=False so it can never block the feed loop that way.
        Critical commands evict the oldest tick when the cap is reached; when the
        queue is entirely critical they may temporarily exceed the cap rather
        than drop an order update.
        """
        label = label or getattr(fn, "__name__", "command")
        if not self.is_running():
            if allow_inline:
                self._run_inline(fn, args, kwargs, label)
                return True
            return False
        cmd = _Command(fn=fn, args=args, kwargs=kwargs, label=label,
                       critical=critical, order_id=order_id)
        with self._cv:
            if self._stop.is_set():
                return False
            total = len(self._critical) + len(self._normal)
            if total >= self.max_queue:
                if critical:
                    if self._normal:
                        self._normal.popleft()
                        self._dropped += 1
                    else:
                        log.error("worker %s critical queue over cap (%d/%d) — "
                                  "keeping command %s", self.name, total,
                                  self.max_queue, label)
                else:
                    self._dropped += 1
                    return False
            (self._critical if critical else self._normal).append(cmd)
            self._submitted += 1
            self._cv.notify()
        (log.info if critical else log.debug)(
            "worker %s %s queued label=%s order=%s",
            self.name, "critical" if critical else "normal", label, order_id or "-")
        return True

    def submit_wait(self, fn, *args, timeout: float | None = None,
                    critical: bool = True, label: str | None = None,
                    order_id: str | None = None, **kwargs):
        """Submit and wait for the result (HTTP/threadpool callers only).

        Never call this from a worker command: it would deadlock. Guarded by
        running inline when the caller is already the worker thread.
        """
        label = label or getattr(fn, "__name__", "command")
        if not self.is_running() or threading.current_thread() is self._thread:
            return self._run_inline(fn, args, kwargs, label)
        wait_sec = self.call_timeout if timeout is None else timeout
        fut: concurrent.futures.Future = concurrent.futures.Future()
        cmd = _Command(fn=fn, args=args, kwargs=kwargs, label=label,
                       critical=critical, order_id=order_id, future=fut)
        with self._cv:
            if self._stop.is_set():
                raise RuntimeError("command worker stopped")
            total = len(self._critical) + len(self._normal)
            if total >= self.max_queue:
                if self._normal:
                    self._normal.popleft()
                    self._dropped += 1
                else:
                    log.error("worker %s queue full (%d/%d) — rejecting %s",
                              self.name, total, self.max_queue, label)
                    raise WorkerQueueFull(
                        f"command queue full ({total}/{self.max_queue})")
            self._critical.append(cmd)
            self._submitted += 1
            self._cv.notify()
        log.info("worker %s critical queued label=%s order=%s wait=%s",
                 self.name, label, order_id or "-", wait_sec)
        try:
            return fut.result(timeout=wait_sec)
        except concurrent.futures.TimeoutError:
            log.error("worker %s command TIMEOUT label=%s order=%s after %ss "
                      "(worker may finish later)", self.name, label,
                      order_id or "-", wait_sec)
            raise TimeoutError(f"{label} timed out after {wait_sec}s") from None

    # ---------- worker loop ----------
    def _run(self):
        while True:
            cmd = self._next()
            if cmd is None:
                return
            started_ms = int(time.time() * 1000)
            started = time.monotonic()
            (log.info if cmd.critical else log.debug)(
                "worker %s sent label=%s order=%s queue_wait=%dms",
                self.name, cmd.label, cmd.order_id or "-",
                max(0, started_ms - cmd.enqueued_ms))
            try:
                result = cmd.fn(*cmd.args, **cmd.kwargs)
            except BaseException as e:  # noqa: BLE001 - report, don't kill the worker
                elapsed_ms = int((time.monotonic() - started) * 1000)
                self._record(cmd, started_ms, elapsed_ms, error=e)
                if cmd.future is not None and not cmd.future.done():
                    cmd.future.set_exception(e)
                log.error("worker %s FAILED label=%s order=%s elapsed=%dms: %s",
                          self.name, cmd.label, cmd.order_id or "-",
                          elapsed_ms, e)
            else:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                self._record(cmd, started_ms, elapsed_ms)
                if cmd.future is not None and not cmd.future.done():
                    cmd.future.set_result(result)
                (log.info if cmd.critical else log.debug)(
                    "worker %s ok label=%s order=%s elapsed=%dms",
                    self.name, cmd.label, cmd.order_id or "-", elapsed_ms)
            if elapsed_ms >= self.stall_ms and not self._stop.is_set():
                log.error("worker %s STALL: command %s order=%s took %dms "
                          "(threshold %dms)", self.name, cmd.label,
                          cmd.order_id or "-", elapsed_ms, self.stall_ms)

    def _next(self) -> _Command | None:
        with self._cv:
            while not self._critical and not self._normal and not self._stop.is_set():
                self._cv.wait()
            if not self._critical and not self._normal:
                return None
            return self._critical.popleft() if self._critical else self._normal.popleft()

    def _run_inline(self, fn, args, kwargs, label):
        started = time.monotonic()
        try:
            return fn(*args, **kwargs)
        finally:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            if elapsed_ms >= self.stall_ms:
                log.error("worker %s STALL (inline): command %s took %dms "
                          "(threshold %dms)", self.name, label, elapsed_ms,
                          self.stall_ms)

    def _record(self, cmd: _Command, started_ms: int, elapsed_ms: int,
                error: BaseException | None = None):
        with self._state_lock:
            self._last_label = cmd.label
            self._last_elapsed_ms = elapsed_ms
            self._last_started_ms = started_ms
            self._last_finished_ms = int(time.time() * 1000)
            if error is not None:
                self._failed += 1
                # /api/health is unauthenticated: expose only the exception type,
                # never raw broker/SDK text that could contain session details.
                self._last_error = type(error).__name__
            else:
                self._completed += 1

    # ---------- observability ----------
    def status(self) -> dict:
        with self._cv:
            pending = len(self._critical) + len(self._normal)
        with self._state_lock:
            return {
                "name": self.name,
                "running": self.is_running(),
                "queue_depth": pending,
                "queue_cap": self.max_queue,
                "submitted": self._submitted,
                "completed": self._completed,
                "failed": self._failed,
                "dropped": self._dropped,
                "last_label": self._last_label,
                "last_elapsed_ms": self._last_elapsed_ms,
                "last_started_ms": self._last_started_ms,
                "last_finished_ms": self._last_finished_ms,
                "last_error": self._last_error,
            }
