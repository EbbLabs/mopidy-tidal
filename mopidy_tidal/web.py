"""HTTP endpoints for managing the user's TIDAL favorites.

Mounted under ``/tidal/`` by Mopidy's HTTP frontend. Routes:

    POST   /tidal/favorites/<kind>s          { "id": "12345" }
    DELETE /tidal/favorites/<kind>s/<id>
    GET    /tidal/favorites/<kind>s

where ``<kind>`` is one of ``album``, ``track``, ``artist``, ``playlist``.

Authentication is implicit: the handlers reuse the ``tidalapi.Session`` already
held by the ``TidalBackend`` actor, so clients don't re-implement OAuth.
"""

from __future__ import unicode_literals

import json
import logging

import pykka
from tornado.web import HTTPError, RequestHandler

logger = logging.getLogger(__name__)

# Tidal entity kinds we expose. Each must map to ``tidalapi.Favorites`` having
# methods ``add_<kind>``, ``remove_<kind>`` and an attribute ``<kind>s``.
KINDS = ("album", "track", "artist", "playlist")


def make_app(config, core):
    """Mopidy ``http:app`` factory. Looks up the running TidalBackend actor
    and binds it into every handler so requests can reach the session."""

    # Late import: backend.py imports from this package at module load and
    # would cause a circular import otherwise.
    from mopidy_tidal.backend import TidalBackend

    refs = pykka.ActorRegistry.get_by_class(TidalBackend)
    if not refs:
        # Should not happen: this factory is only invoked when the extension
        # is enabled, which also instantiates the backend.
        raise RuntimeError(
            "TidalBackend actor not running; cannot register HTTP routes"
        )
    backend = refs[0].proxy()

    common = {"backend": backend}
    return [
        (
            r"/favorites/(album|track|artist|playlist)s",
            FavoritesCollectionHandler,
            common,
        ),
        (
            r"/favorites/(album|track|artist|playlist)s/([^/]+)",
            FavoritesItemHandler,
            common,
        ),
    ]


class _Base(RequestHandler):
    def initialize(self, backend):
        self.backend = backend

    def write_error(self, status_code, **kwargs):
        self.set_header("Content-Type", "application/json")
        self.finish(json.dumps({"error": self._reason}))

    def _session(self):
        # ``backend.session`` is a property that drives the (lazy) login flow.
        # Going through the Pykka proxy serialises access on the backend
        # actor thread; ``.get()`` unwraps the Future on the IOLoop thread.
        try:
            session = self.backend.session.get()
        except Exception as e:
            logger.exception("tidal session unavailable")
            raise HTTPError(503, reason=f"tidal session unavailable: {e}")
        if session is None or getattr(session, "user", None) is None:
            raise HTTPError(503, reason="tidal session not logged in")
        return session


class FavoritesCollectionHandler(_Base):
    def get(self, kind):
        session = self._session()
        items = getattr(session.user.favorites, f"{kind}s")()
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps([_summarize(x) for x in items or []]))

    def post(self, kind):
        session = self._session()
        try:
            body = json.loads(self.request.body or b"{}")
        except json.JSONDecodeError as e:
            raise HTTPError(400, reason=f"invalid JSON: {e}")
        item_id = body.get("id")
        if not item_id:
            raise HTTPError(400, reason="missing 'id' in body")
        getattr(session.user.favorites, f"add_{kind}")(item_id)
        self.set_status(204)


class FavoritesItemHandler(_Base):
    def delete(self, kind, item_id):
        session = self._session()
        getattr(session.user.favorites, f"remove_{kind}")(item_id)
        self.set_status(204)


def _summarize(obj):
    """tidalapi objects don't JSON-serialise cleanly; pull a small summary."""
    summary = {"id": str(getattr(obj, "id", ""))}
    name = getattr(obj, "name", None)
    if name:
        summary["name"] = name
    artist = getattr(obj, "artist", None)
    if artist is not None:
        summary["artist"] = getattr(artist, "name", None)
    return summary
