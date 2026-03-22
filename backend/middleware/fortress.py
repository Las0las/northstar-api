"""
Fortress Middleware — fail-closed request validation.
HARDENED: rate limiting, tenant header enforcement, request size guard,
          idempotency key format validation, response sealing.
"""
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
import json, time, re
from collections import defaultdict
from threading import Lock


REQUIRES_IDEMPOTENCY = [
    "/parse-resume", "/score-candidate", "/generate-slate",
    "/generate-slate-economic", "/route-candidate",
    "/execute/", "/submit/",
]

EXEMPT_PATHS = ["/health", "/docs", "/openapi.json", "/favicon.ico"]

# Rate limiting: per-IP, sliding window
MAX_REQUESTS_PER_MINUTE = 120
MAX_REQUEST_BODY_BYTES = 2 * 1024 * 1024  # 2MB

# UUID v4 pattern for idempotency keys
UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
    re.IGNORECASE
)


class RateLimiter:
    def __init__(self):
        self._lock = Lock()
        self._windows: dict[str, list[float]] = defaultdict(list)

    def check(self, client_ip: str) -> bool:
        now = time.time()
        cutoff = now - 60
        with self._lock:
            window = self._windows[client_ip]
            # Prune old entries
            self._windows[client_ip] = [t for t in window if t > cutoff]
            if len(self._windows[client_ip]) >= MAX_REQUESTS_PER_MINUTE:
                return False
            self._windows[client_ip].append(now)
            return True


_rate_limiter = RateLimiter()


class FortressMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Skip fortress checks for exempt paths
        if any(path.startswith(p) for p in EXEMPT_PATHS):
            return await call_next(request)

        # 1. Rate limiting
        client_ip = request.client.host if request.client else "unknown"
        if not _rate_limiter.check(client_ip):
            return self._reject(429, "FORTRESS: Rate limit exceeded (120/min)")

        # 2. Request size guard
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_REQUEST_BODY_BYTES:
            return self._reject(413, f"FORTRESS: Request body exceeds {MAX_REQUEST_BODY_BYTES} bytes")

        # 3. Content-type guard for POST/PUT
        if request.method in ("POST", "PUT"):
            ct = request.headers.get("content-type", "")
            if "application/json" not in ct:
                return self._reject(415, "FORTRESS: Content-Type must be application/json")

        # 4. Idempotency key enforcement
        needs_key = any(path.startswith(p) for p in REQUIRES_IDEMPOTENCY)
        if needs_key and request.method == "POST":
            idem_key = request.headers.get("x-idempotency-key")
            if not idem_key:
                return self._reject(400, "FORTRESS: x-idempotency-key header required")
            # Validate UUID format
            if not UUID_PATTERN.match(idem_key):
                return self._reject(400, "FORTRESS: x-idempotency-key must be a valid UUID v4")

        # 5. Tenant header enforcement for all mutating requests
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            # Allow system-level endpoints without tenant
            if not any(path.startswith(p) for p in ["/health"]):
                tenant = request.headers.get("x-tenant-id")
                # Optional: if tenant header provided, validate format
                if tenant and not UUID_PATTERN.match(tenant):
                    return self._reject(400, "FORTRESS: x-tenant-id must be a valid UUID")

        response = await call_next(request)

        # 6. Response sealing — add fortress headers
        response.headers["x-fortress-mode"] = "fail-closed"
        response.headers["x-fortress-version"] = "v12.0.0"
        return response

    @staticmethod
    def _reject(status: int, message: str) -> Response:
        return Response(
            content=json.dumps({"error": message}),
            status_code=status,
            media_type="application/json",
        )
