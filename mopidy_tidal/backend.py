from __future__ import unicode_literals

import logging
import time
from concurrent.futures import Future
from pathlib import Path
from typing import Optional, Union

from mopidy import backend
from pykka import ThreadingActor
from tidalapi import Config, Session
from tidalapi import __version__ as tidalapi_ver

from mopidy_tidal import Extension, context, library, playback, playlists
from mopidy_tidal import __version__ as mopidy_tidal_ver
from mopidy_tidal.web_auth_server import WebAuthServer

logger = logging.getLogger(__name__)


class TidalBackend(ThreadingActor, backend.Backend):
    def __init__(self, config, audio):
        super().__init__()
        # Mopidy cfg
        self._config = config
        context.set_config(self._config)
        self._tidal_config = config[Extension.ext_name]

        # Backend
        self.playback = playback.TidalPlaybackProvider(audio=audio, backend=self)
        self.library = library.TidalLibraryProvider(backend=self)
        self.playlists = playlists.TidalPlaylistsProvider(backend=self)

        # Session parameters
        self._active_session: Optional[Session] = None
        self._logged_in: bool = False
        self.uri_schemes: tuple[str] = ("tidal",)
        self._login_future: Optional[Future] = None
        self._login_url: Optional[str] = None
        self.data_dir: Path = Path(Extension.get_data_dir(self._config))
        self.session_file_path: Path = Path("")
        self.web_auth_server: WebAuthServer = WebAuthServer()

        # Config parameters
        # Lazy: Connect lazily, i.e. login only when user starts browsing TIDAL directories
        self.lazy_connect: bool = False
        # Login Method:
        #   BLOCK:       Immediately prompt user for login (This will block mopidy startup!)
        #   HACK/AUTO:   Display dummy track with login URL. When clicked, QR code and TTS is generated
        self.login_method: str = "BLOCK"
        # pkce_enabled: If true, TIDAL session will use PKCE auth. Otherwise OAuth2 is used
        self.auth_method: str = "OAUTH"
        self.pkce_enabled: bool = False
        # login_server_port: Port to use for login HTTP server, eg. <host_ip>:<port>. Default <host_ip>:8989
        self.login_server_port: int = 8989

    @property
    def session(self):
        if not self.logged_in:
            self._login()
        return self._active_session

    @property
    def logged_in(self):
        if not self._logged_in:
            if self._active_session.load_session_from_file(self.session_file_path):
                logger.info("Loaded TIDAL session from file %s", self.session_file_path)
                self._logged_in = self.session_valid
        return self._logged_in

    @property
    def session_valid(self):
        # Returns true when session is logged in and valid
        return self._active_session.check_login()

    def on_start(self):
        logger.info("Mopidy-Tidal version: v%s", mopidy_tidal_ver)
        logger.info("Python-Tidal version: v%s", tidalapi_ver)
        quality = self._tidal_config["quality"]
        client_id = self._tidal_config["client_id"]
        client_secret = self._tidal_config["client_secret"]
        self.auth_method = self._tidal_config["auth_method"]
        if self.auth_method == "PKCE":
            self.pkce_enabled = True
        self.login_server_port = self._tidal_config["login_server_port"]
        logger.info("PKCE login web server port: %s", self.login_server_port)
        self.login_method = self._tidal_config["login_method"]
        if self.login_method == "AUTO":
            # Add AUTO as alias to HACK login method
            self.login_method = "HACK"
        self.lazy_connect = self._tidal_config["lazy"]
        logger.info("Quality: %s", quality)
        logger.info("Authentication: %s", "PKCE" if self.pkce_enabled else "OAuth")
        config = Config(quality=quality)

        # Set the session filename, depending on the type of session
        if self.pkce_enabled:
            self.session_file_path = Path(self.data_dir, "tidal-pkce.json")
        else:
            self.session_file_path = Path(self.data_dir, "tidal-oauth.json")

        if (self.login_method == "HACK") and not self._tidal_config["lazy"]:
            logger.warning("AUTO login implies lazy connection, setting lazy=True.")
            self.lazy_connect = True
        logger.info(
            "Login method: %s", "BLOCK" if self.pkce_enabled == "BLOCK" else "AUTO"
        )

        if client_id and client_secret:
            logger.info("Using client id & client secret from config")
            config.client_id = client_id
            config.client_secret = client_secret
        elif (client_id and not client_secret) or (client_secret and not client_id):
            logger.warning("Always provide both client_id and client_secret")
            logger.info("Using default client id & client secret from python-tidal")
        else:
            logger.info("Using default client id & client secret from python-tidal")

        self._active_session = Session(config)
        if not self.lazy_connect:
            self._login()

    def _login(self):
        """Load session at startup or create a new session"""
        if self._active_session.load_session_from_file(self.session_file_path):
            logger.info(
                "Loaded existing TIDAL session from file %s...", self.session_file_path
            )
        if not self.session_valid:
            if not self.login_server_port:
                # A. Default login, user must find login URL in Mopidy log
                logger.info("Creating new session (OAuth)...")
                self._active_session.login_oauth_simple(fn_print=logger.info)
            else:
                # B. Interactive login, user must perform login using web auth
                logger.info(
                    "Creating new session (%s)...",
                    "PKCE" if self.pkce_enabled else "OAuth",
                )
                if self.pkce_enabled:
                    # PKCE Login
                    login_url = self._active_session.pkce_login_url()
                    logger.info(
                        "Please visit 'http://localhost:%s' to authenticate",
                        self.login_server_port,
                    )
                    # Enable web server for interactive login + callback on form Submit
                    self.web_auth_server.set_callback(self._web_auth_callback)
                    self.web_auth_server.start_oauth_daemon(
                        login_url, self.login_server_port, self.pkce_enabled
                    )
                else:
                    # OAuth login
                    login_url = self.login_url
                    logger.info(
                        "Please visit 'http://localhost:%s' or '%s' to authenticate",
                        self.login_server_port,
                        login_url,
                    )
                    # Enable web server for interactive login (no callback)
                    self.web_auth_server.start_oauth_daemon(
                        login_url, self.login_server_port, self.pkce_enabled
                    )

                # Wait for user to complete interactive login sequence
                max_time = time.time() + 300
                while time.time() < max_time:
                    if self._logged_in:
                        if not self.pkce_enabled:
                            self._complete_login()
                        return
                    logger.info(
                        "Time left to complete authentication: %s sec",
                        int(max_time - time.time()),
                    )
                    time.sleep(5)
                raise TimeoutError("You took too long to log in")

    def _web_auth_callback(self, url_redirect: str):
        """Callback triggered on web auth completion
        :param url_redirect: URL of the 'Ooops' page, where the user was redirected to after login.
        :type url_redirect: str
        """
        if self.pkce_enabled:
            try:
                # Query for auth tokens
                json: dict[str, Union[str, int]] = (
                    self._active_session.pkce_get_auth_token(url_redirect)
                )
                # Parse and set tokens.
                self._active_session.process_auth_token(json, is_pkce_token=True)
                self._logged_in = True
            except Exception:
                raise ValueError("Response code is required for PKCE login!")
        # Store session after auth completion
        self._complete_login()

    def _complete_login(self):
        """Perform final steps of login sequence; save session to file"""
        if self.session_valid:
            # Only store current session if valid
            logger.info("TIDAL Login OK")
            self._active_session.save_session_to_file(self.session_file_path)
            self._logged_in = True
        else:
            logger.error("TIDAL Login Failed")
            raise ConnectionError("Failed to log in.")

    @property
    def logging_in(self) -> bool:
        """Are we currently waiting for user confirmation to log in?"""
        return bool(self._login_future and self._login_future.running())

    @property
    def login_url(self) -> Optional[str]:
        """Start a new login sequence (if not active) and get the latest login URL"""
        if not self.pkce_enabled:
            if not self._logged_in and not self.logging_in:
                login_url, self._login_future = self._active_session.login_oauth()
                self._login_future.add_done_callback(lambda *_: self._complete_login())
                self._login_url = login_url.verification_uri_complete
            return f"https://{self._login_url}" if self._login_url else None
        else:
            if not self._logged_in and not self.web_auth_server.is_daemon_running:
                login_url = self._active_session.pkce_login_url()
                self._login_url = "http://localhost:{}".format(self.login_server_port)
                # Enable web server for interactive login + callback on form Submit
                self.web_auth_server.set_callback(self._web_auth_callback)
                self.web_auth_server.start_oauth_daemon(
                    login_url, self.login_server_port, self.pkce_enabled
                )
            return f"{self._login_url}" if self._login_url else None
