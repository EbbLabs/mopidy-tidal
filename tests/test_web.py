"""Tests for the HTTP favorites API in :mod:`mopidy_tidal.web`."""

from __future__ import unicode_literals

import json
from unittest.mock import MagicMock, Mock

import pytest
from tornado.httputil import HTTPServerRequest
from tornado.web import Application, HTTPError

from mopidy_tidal import web

# ── helpers ────────────────────────────────────────────────────────────────


def _make_request(body=b"", method="GET"):
    request = Mock(spec=HTTPServerRequest)
    request.method = method
    request.body = body
    request.headers = {}
    request.connection = Mock()
    request.uri = "/"
    request.host = "localhost"
    request.full_url = lambda: "http://localhost/"
    request.supports_http_1_1 = lambda: True
    request.version = "HTTP/1.1"
    return request


def _make_handler(handler_cls, backend, body=b"", method="GET"):
    """Instantiate a Tornado RequestHandler bypassing the IOLoop plumbing.

    We override the side-effectful response methods (``write``, ``set_status``,
    ``set_header``, ``finish``) with mocks so the test can inspect what the
    handler tried to send.
    """
    app = Application()
    request = _make_request(body=body, method=method)
    handler = handler_cls(app, request, backend=backend)
    handler.write = Mock()
    handler.set_status = Mock()
    handler.set_header = Mock()
    handler.finish = Mock()
    return handler


def _written_json(handler):
    handler.write.assert_called_once()
    payload = handler.write.call_args.args[0]
    return json.loads(payload)


# ── fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def favorites():
    return MagicMock()


@pytest.fixture
def session(favorites):
    user = Mock(favorites=favorites)
    return Mock(user=user)


@pytest.fixture
def backend(session):
    """A stand-in for the Pykka proxy returned by ``ActorRegistry``.

    ``backend.session`` returns a stub Future whose ``.get()`` yields the
    mocked :class:`tidalapi.Session`.
    """
    future = Mock()
    future.get.return_value = session
    proxy = Mock()
    proxy.session = future
    return proxy


# ── _summarize ─────────────────────────────────────────────────────────────


def test_summarize_minimal_object():
    assert web._summarize(Mock(spec=[])) == {"id": ""}


def test_summarize_includes_name_when_present():
    obj = Mock(spec=["id", "name"], id=42)
    obj.name = "Discovery"
    result = web._summarize(obj)
    assert result == {"id": "42", "name": "Discovery"}


def test_summarize_includes_artist_name_when_present():
    artist = Mock(name="Daft Punk")
    artist.name = "Daft Punk"
    obj = Mock(id=42, name="Discovery", artist=artist)
    result = web._summarize(obj)
    assert result["artist"] == "Daft Punk"


# ── make_app ───────────────────────────────────────────────────────────────


def test_make_app_returns_route_specs(mocker):
    backend_ref = Mock()
    backend_ref.proxy.return_value = Mock()
    mocker.patch.object(
        web.pykka.ActorRegistry, "get_by_class", return_value=[backend_ref]
    )

    routes = web.make_app(config={}, core=Mock())

    assert len(routes) == 2
    pattern_collection, handler_collection, init_collection = routes[0]
    pattern_item, handler_item, init_item = routes[1]
    assert handler_collection is web.FavoritesCollectionHandler
    assert handler_item is web.FavoritesItemHandler
    assert "backend" in init_collection
    assert "backend" in init_item


def test_make_app_raises_when_no_backend(mocker):
    mocker.patch.object(web.pykka.ActorRegistry, "get_by_class", return_value=[])

    with pytest.raises(RuntimeError, match="TidalBackend"):
        web.make_app(config={}, core=Mock())


# ── _Base._session ─────────────────────────────────────────────────────────


def test_session_returns_session_when_logged_in(backend, session):
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    assert handler._session() is session


def test_session_raises_503_when_session_is_none(backend):
    backend.session.get.return_value = None
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    with pytest.raises(HTTPError) as exc:
        handler._session()
    assert exc.value.status_code == 503


def test_session_raises_503_when_user_missing(backend):
    backend.session.get.return_value = Mock(user=None)
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    with pytest.raises(HTTPError) as exc:
        handler._session()
    assert exc.value.status_code == 503


def test_session_raises_503_when_proxy_raises(backend):
    backend.session.get.side_effect = TimeoutError("login timed out")
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    with pytest.raises(HTTPError) as exc:
        handler._session()
    assert exc.value.status_code == 503


# ── FavoritesCollectionHandler.get ─────────────────────────────────────────


@pytest.mark.parametrize("kind", web.KINDS)
def test_collection_get_returns_summaries(backend, favorites, kind):
    item = Mock(spec=["id", "name"], id=42)
    item.name = "Discovery"
    getattr(favorites, f"{kind}s").return_value = [item]

    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    handler.get(kind)

    handler.set_header.assert_called_with("Content-Type", "application/json")
    payload = _written_json(handler)
    assert payload == [{"id": "42", "name": "Discovery"}]


def test_collection_get_handles_empty(backend, favorites):
    favorites.albums.return_value = []
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    handler.get("album")
    assert _written_json(handler) == []


def test_collection_get_tolerates_none_from_tidal(backend, favorites):
    """``tidalapi`` occasionally returns ``None`` instead of an empty list."""
    favorites.albums.return_value = None
    handler = _make_handler(web.FavoritesCollectionHandler, backend)
    handler.get("album")
    assert _written_json(handler) == []


# ── FavoritesCollectionHandler.post ────────────────────────────────────────


@pytest.mark.parametrize("kind", web.KINDS)
def test_collection_post_adds_favorite(backend, favorites, kind):
    handler = _make_handler(
        web.FavoritesCollectionHandler,
        backend,
        body=b'{"id": "12345"}',
        method="POST",
    )
    handler.post(kind)
    getattr(favorites, f"add_{kind}").assert_called_once_with("12345")
    handler.set_status.assert_called_once_with(204)


def test_collection_post_rejects_invalid_json(backend):
    handler = _make_handler(
        web.FavoritesCollectionHandler,
        backend,
        body=b"not-json",
        method="POST",
    )
    with pytest.raises(HTTPError) as exc:
        handler.post("album")
    assert exc.value.status_code == 400


def test_collection_post_rejects_missing_id(backend):
    handler = _make_handler(
        web.FavoritesCollectionHandler,
        backend,
        body=b"{}",
        method="POST",
    )
    with pytest.raises(HTTPError) as exc:
        handler.post("album")
    assert exc.value.status_code == 400


def test_collection_post_rejects_empty_body(backend):
    handler = _make_handler(
        web.FavoritesCollectionHandler,
        backend,
        body=b"",
        method="POST",
    )
    with pytest.raises(HTTPError) as exc:
        handler.post("album")
    assert exc.value.status_code == 400


# ── FavoritesItemHandler.delete ────────────────────────────────────────────


@pytest.mark.parametrize("kind", web.KINDS)
def test_item_delete_removes_favorite(backend, favorites, kind):
    handler = _make_handler(web.FavoritesItemHandler, backend, method="DELETE")
    handler.delete(kind, "12345")
    getattr(favorites, f"remove_{kind}").assert_called_once_with("12345")
    handler.set_status.assert_called_once_with(204)


# ── write_error ────────────────────────────────────────────────────────────


def test_write_error_emits_json():
    handler = _make_handler(web.FavoritesCollectionHandler, Mock())
    handler._reason = "boom"
    handler.write_error(500)
    handler.set_header.assert_called_with("Content-Type", "application/json")
    handler.finish.assert_called_once()
    payload = json.loads(handler.finish.call_args.args[0])
    assert payload == {"error": "boom"}
