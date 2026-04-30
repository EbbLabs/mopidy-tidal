from json import dump, dumps, load, loads
from pathlib import Path

import pytest

from mopidy_tidal.library import TidalLibraryProvider
from mopidy_tidal.playback import TidalPlaybackProvider
from mopidy_tidal.playlists import TidalPlaylistsProvider


def test_backend_composed_of_correct_parts(get_backend):
    backend, *_ = get_backend()

    assert isinstance(backend.playback, TidalPlaybackProvider)
    assert isinstance(backend.library, TidalLibraryProvider)
    assert isinstance(backend.playlists, TidalPlaylistsProvider)


def test_backend_begins_in_correct_state(get_backend):
    """This test tests private attributes and is thus *BAD*.  But we can keep
    it till it breaks."""
    backend, config, *_ = get_backend()

    assert backend.uri_schemes == ("tidal",)
    with pytest.raises(ValueError):
        backend._active_session.get()
    assert backend._config is config


def test_initial_login_caches_credentials(get_backend, config):
    backend, _, _, _, session = get_backend(config=config)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Starting the mock web server will trigger login instantly
    backend.web_auth_server.start_oauth_daemon.side_effect = login_success

    backend._active_session.set(session)
    authf = Path(config["core"]["data_dir"], "tidal/tidal-oauth.json")
    assert not authf.exists()

    # backend._login()
    backend.on_start()

    # First attempt to (mock) load session from file (fails)
    session.load_session_from_file.assert_called_once()
    # Followed by OAuth (mock) daemon starting
    backend.web_auth_server.start_oauth_daemon.assert_called_once()
    # After a succesful (mock) oauth, the session file should be created
    session.save_session_to_file.assert_called_once()

    # Check if dummy file was created with the expected contents
    with authf.open() as f:
        data = load(f)
        assert data == {
            "token_type": dict(data="token_type"),
            "session_id": dict(data="session_id"),
            "access_token": dict(data="access_token"),
            "refresh_token": dict(data="refresh_token"),
            "is_pkce": dict(data=False),
        }


def test_login_after_failed_cached_credentials_overwrites_cached_credentials(
    get_backend, config
):
    backend, _, _, _, session = get_backend(config=config)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Starting the mock web server will trigger login instantly
    backend.web_auth_server.start_oauth_daemon.side_effect = login_success

    backend._active_session.set(session)
    authf = Path(config["core"]["data_dir"], "tidal/tidal-oauth.json")
    cached_data = {
        "token_type": dict(data="token_type2"),
        "session_id": dict(data="session_id2"),
        "access_token": dict(data="access_token2"),
        "refresh_token": dict(data="refresh_token2"),
        "is_pkce": dict(data=False),
    }
    authf.write_text(dumps(cached_data))

    backend.on_start()

    with authf.open() as f:
        data = load(f)
    assert data == {
        "token_type": dict(data="token_type"),
        "session_id": dict(data="session_id"),
        "access_token": dict(data="access_token"),
        "refresh_token": dict(data="refresh_token"),
        "is_pkce": dict(data=False),
    }

    # After a succesful (mock) oauth, the session file should be created (overwriting original file)
    session.save_session_to_file.assert_called_once()


def test_failed_login_does_not_overwrite_cached_credentials(
    get_backend, mocker, config, tmp_path
):
    backend, _, _, _, session = get_backend(config=config)

    # trigger failed login by setting check_login false even though oauth was completed
    def login_failed(*_, **__):
        # session.check_login.return_value = False
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Starting the mock web server will trigger login instantly (but login will fail)
    backend.web_auth_server.start_oauth_daemon.side_effect = login_failed

    backend._active_session.set(session)
    authf = Path(config["core"]["data_dir"], "tidal/tidal-oauth.json")
    cached_data = {
        "token_type": dict(data="token_type2"),
        "session_id": dict(data="session_id2"),
        "access_token": dict(data="access_token2"),
        "refresh_token": dict(data="refresh_token2"),
    }
    authf.write_text(dumps(cached_data))

    with pytest.raises(ConnectionError):
        backend.on_start()

    data = loads(authf.read_text())
    assert data == cached_data

    # First attempt to (mock) load session from file (fails)
    session.load_session_from_file.assert_called_once()
    # Followed by OAuth (mock) daemon starting
    backend.web_auth_server.start_oauth_daemon.assert_called_once()
    # Login failed, no session file created
    session.save_session_to_file.assert_not_called()


def test_failed_overall_login_throws_error(get_backend, tmp_path, mocker, config):
    backend, _, _, _, session = get_backend(config=config)

    # trigger failed login by setting check_login false even though oauth was completed
    def login_failed(*_, **__):
        # session.check_login.return_value = False
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Starting the mock web server will trigger login instantly (but login will fail)
    backend.web_auth_server.start_oauth_daemon.side_effect = login_failed

    backend._active_session.set(session)
    authf = tmp_path / "auth.json"

    with pytest.raises(ConnectionError):
        backend.on_start()

    assert not authf.exists()


def test_logs_in(get_backend, mocker, config):
    backend, _, _, session_factory, session = get_backend(config=config)
    backend._active_session.set(session)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Starting the mock web server will trigger login instantly
    backend.web_auth_server.start_oauth_daemon.side_effect = login_success

    backend.on_start()

    session_factory.assert_called_once()
    config_obj = session_factory.mock_calls[0].args[0]
    assert config_obj.quality == config["tidal"]["quality"]
    assert config_obj.client_id == config["tidal"]["client_id"]
    assert config_obj.client_secret == config["tidal"]["client_secret"]


def test_accessing_session_triggers_lazy_login(get_backend, mocker, config):
    config["tidal"]["lazy"] = True
    backend, _, _, session_factory, session = get_backend(config=config)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Loading session from file will result in successful login
    session.load_session_from_file.side_effect = login_success

    backend.on_start()

    session.load_session_from_file.assert_not_called()
    assert not backend._active_session.get().check_login()
    assert backend.session  # accessing session will trigger login
    assert backend.session.check_login()
    session_factory.assert_called_once()
    session.load_session_from_file.assert_called_once()
    config_obj = session_factory.mock_calls[0].args[0]
    assert config_obj.quality == config["tidal"]["quality"]
    assert config_obj.client_id == config["tidal"]["client_id"]
    assert config_obj.client_secret == config["tidal"]["client_secret"]


def test_logs_in_only_client_secret(get_backend, mocker, config):
    config["tidal"]["client_id"] = ""
    backend, _, _, session_factory, session = get_backend(config=config)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Loading session from file will result in successful login
    session.load_session_from_file.side_effect = login_success

    backend.on_start()

    session.load_session_from_file.assert_called_once()
    session_factory.assert_called_once()
    config_obj = session_factory.mock_calls[0].args[0]
    assert config_obj.quality == config["tidal"]["quality"]
    assert config_obj.client_id and config_obj.client_id != config["tidal"]["client_id"]
    assert (
        config_obj.client_secret
        and config_obj.client_secret != config["tidal"]["client_secret"]
    )


def test_logs_in_default_id_secret(get_backend, mocker, config):
    config["tidal"]["client_id"] = ""
    config["tidal"]["client_secret"] = ""
    backend, _, _, session_factory, session = get_backend(config=config)

    def login_success(*_, **__):
        session.check_login.return_value = True
        backend._logged_in = True  # Set to true to skip login timeout immediately

    # Loading session from file will result in successful login
    session.load_session_from_file.side_effect = login_success

    backend.on_start()

    session.load_session_from_file.assert_called_once()
    session_factory.assert_called_once()
    config_obj = session_factory.mock_calls[0].args[0]
    assert config_obj.quality == config["tidal"]["quality"]
    assert config_obj.client_id and config_obj.client_id != config["tidal"]["client_id"]
    assert (
        config_obj.client_secret
        and config_obj.client_secret != config["tidal"]["client_secret"]
    )


def test_loads_session(get_backend, mocker, config):
    backend, config, _, session_factory, session = get_backend(config=config)

    authf = Path(config["core"]["data_dir"], "tidal") / "tidal-oauth.json"
    data = {
        "token_type": dict(data="token_type"),
        "session_id": dict(data="session_id"),
        "access_token": dict(data="access_token"),
        "refresh_token": dict(data="refresh_token"),
    }
    with authf.open("w") as f:
        dump(data, f)
    session.check_login.return_value = True

    backend.on_start()

    # Loading session successfully should not trigger oauth_daemon
    backend.web_auth_server.start_oauth_daemon.assert_not_called()
    session.load_session_from_file.assert_called_once()
    session_factory.assert_called_once()
