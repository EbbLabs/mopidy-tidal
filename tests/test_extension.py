from __future__ import unicode_literals

from mopidy_tidal import Extension
from mopidy_tidal.backend import TidalBackend


def test_sanity_check_default_resembles_conf_file():
    ext = Extension()

    config = ext.get_default_config()

    assert "[tidal]" in config
    assert "enabled = true" in config


def test_config_schema_has_correct_keys():
    """This is mostly a sanity check in case we forget to add a key."""
    ext = Extension()

    schema = ext.get_config_schema()

    assert set(schema.keys()) == {
        "enabled",
        "quality",
        "client_id",
        "client_secret",
        "playlist_cache_refresh_secs",
        "lazy",
        "login_method",
        "login_server_port",
        "auth_method",
        "playback_cache",
        "playback_cache_max_entries",
        "playback_cache_buffer_bytes",
    }


def test_extension_setup_registers_tidal_backend(mocker):
    ext = Extension()
    registry = mocker.Mock()

    ext.setup(registry)

    backend_calls = [
        c for c in registry.add.mock_calls if c.args and c.args[0] == "backend"
    ]
    assert len(backend_calls) == 1
    _, obj = backend_calls[0].args
    assert issubclass(obj, TidalBackend)


def test_extension_setup_registers_http_app(mocker):
    from mopidy_tidal.web import make_app

    ext = Extension()
    registry = mocker.Mock()

    ext.setup(registry)

    http_calls = [
        c for c in registry.add.mock_calls if c.args and c.args[0] == "http:app"
    ]
    assert len(http_calls) == 1
    _, spec = http_calls[0].args
    assert spec["name"] == "tidal"
    assert spec["factory"] is make_app
