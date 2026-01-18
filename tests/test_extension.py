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
        "track_cache",
        "track_cache_max_entries",
    }


def test_extension_setup_registers_tidal_backend(mocker):
    ext = Extension()
    registry = mocker.Mock()

    ext.setup(registry)

    registry.add.assert_called_once()
    plugin_type, obj = registry.add.mock_calls[0].args
    assert plugin_type == "backend"
    assert issubclass(obj, TidalBackend)
