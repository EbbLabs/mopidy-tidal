import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer
from string import whitespace
from typing import Callable, Optional
from urllib.parse import unquote

HTML_BODY = """<!DOCTYPE html>
<html>
<head>
<title>TIDAL Web Auth</title>
</head>
<body>

<h1>KEEP THIS TAB OPEN</h1>
<a href={authurl} target="_blank" rel="noopener noreferrer">Click here to be forwarded to TIDAL Login page</a>
{interactive}

</body>
</html>
""".format

INTERACTIVE_HTML_BODY = """
<p>...then, after login, copy the complete URL of the page you were redirected to.</p>
<p>Probably a "Oops / Not found" page, nevertheless we need the whole URL as is.</p>
<form method="post">
  <label for="code">Paste the response URL here:</label>
  <input type="url" id="code" name="code">
  <input type="submit" value="Submit">
</form>
"""


class WebAuthServer:
    def __init__(self):
        self.handler: Optional[partial] = None
        self.callback: Optional[Callable] = None
        self.response_code: str = ""
        self.daemon_started: bool = False

    def start_oauth_daemon(self, login_url: str, port: int, pkce_enabled: bool):
        if self.daemon_started:
            return

        self.handler = partial(
            HTTPHandler, login_url, self.set_response_code, pkce_enabled
        )

        daemon = threading.Thread(
            name="TidalOAuthLogin",
            target=HTTPServer(("", port), self.handler).serve_forever,
        )
        daemon.daemon = (
            True  # Set as a daemon so it will be killed once the main thread is dead.
        )
        daemon.start()
        self.daemon_started = True

    def set_callback(self, callback: Callable[[str], None]):
        self.callback = callback

    def set_response_code(self, response_code: str):
        self.response_code = response_code
        if self.callback:
            self.callback(response_code)

    @property
    def is_daemon_running(self):
        return self.daemon_started

    @property
    def get_response_code(self):
        if self.response_code == "":
            return None
        else:
            return self.response_code


class HTTPHandler(BaseHTTPRequestHandler, object):
    def __init__(
        self,
        login_url: str,
        callback: Callable[[str], None],
        pkce_enabled: bool,
        *args,
        **kwargs,
    ):
        self.login_url = login_url
        self.callback_fn: Callable = callback
        self.pkce_enabled = pkce_enabled
        super().__init__(*args, **kwargs)

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        interactive = INTERACTIVE_HTML_BODY if self.pkce_enabled else ""
        self.wfile.write(
            HTML_BODY(authurl=self.login_url, interactive=interactive).encode()
        )

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length"), 0)
        body = self.rfile.read(content_length).decode()
        try:
            form = {k: v for k, v in (p.split("=", 1) for p in body.split("&"))}
            code_url = unquote(form["code"].strip(whitespace))
        except:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Malformed request")
            raise
        else:
            self.callback_fn(code_url)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Success!\nEnjoy your music!")
