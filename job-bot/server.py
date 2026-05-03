import os
import queue
import threading
import subprocess
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler

log = logging.getLogger(__name__)

_q: queue.Queue = queue.Queue(maxsize=1)
_running = threading.Event()


def _worker():
    while True:  # pragma: no cover
        mode = _q.get()
        if _running.is_set():  # pragma: no cover
            log.info(f"[Server] already running, dropping '{mode}'")  # pragma: no cover
            _q.task_done()  # pragma: no cover
            continue  # pragma: no cover
        _running.set()  # pragma: no cover
        try:  # pragma: no cover
            log.info(f"[Server] starting: python main.py {mode}")  # pragma: no cover
            subprocess.run(["python", "main.py", mode], env={**os.environ})  # pragma: no cover
        finally:  # pragma: no cover
            _running.clear()  # pragma: no cover
            _q.task_done()  # pragma: no cover


threading.Thread(target=_worker, daemon=True).start()


class H(BaseHTTPRequestHandler):
    def _respond(self, code: int, body: bytes):
        self.send_response(code)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            status = b"running" if _running.is_set() else b"idle"
            self._respond(200, status)
        elif self.path.startswith("/run"):
            mode = self.path.split("?mode=")[-1] if "?mode=" in self.path else "full"
            try:
                _q.put_nowait(mode)
                self._respond(202, f"queued:{mode}".encode())
            except queue.Full:
                self._respond(409, b"already queued")
        else:
            self._respond(404, b"not found")

    def log_message(self, *_):
        pass


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)  # pragma: no cover
    port = int(os.environ.get("PORT", 8000))  # pragma: no cover
    log.info(f"[Server] listening on :{port}")  # pragma: no cover
    HTTPServer(("0.0.0.0", port), H).serve_forever()  # pragma: no cover
