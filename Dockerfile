# Single image: build React, serve via FastAPI (uvicorn). Python deps via uv.
FROM node:20-slim AS fe
WORKDIR /fe
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ ./
RUN npm run build

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS be
WORKDIR /srv
# docker-compose pins TZ=Asia/Kolkata for IST logs; the slim base omits the
# timezone database, so install it explicitly.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*
ENV UV_FROZEN=1 UV_NO_DEV=1
COPY backend/pyproject.toml backend/uv.lock ./backend/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --project backend --no-install-project
COPY backend/ ./backend/
COPY --from=fe /fe/dist ./frontend/dist
ENV PYTHONPATH=/srv/backend PATH="/srv/backend/.venv/bin:$PATH"
EXPOSE 8000
# Port 8000 is only reachable from Caddy on the internal compose network, so the
# proxy's X-Forwarded-For is trusted for the client IP used by the allowlist.
CMD ["uv", "run", "--project", "backend", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
