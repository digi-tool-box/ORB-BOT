from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
import os

class KeepAliveHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; charset=utf-8')
            self.end_headers()
            self.wfile.write(b"Trading Bot is Alive and Running!")
        else:
            self.send_response(404)
            self.end_headers()


def run():
    # start a simple HTTP server on the given port
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), KeepAliveHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()


def keep_alive():
    t = Thread(target=run, daemon=True)
    t.start()