from copy import deepcopy
from time import sleep

import pytest
from mopidy.models import Track
from requests import HTTPError

from mopidy_tidal.playlists import (
    MopidyPlaylist,
    PlaylistCache,
    Ref,
    TidalPlaylist,
    TidalPlaylistsProvider,
)


@pytest.fixture
def tpp(mocker):
    mocker.patch("mopidy_tidal.playlists.Timer")
    backend = mocker.Mock()
    backend._config = {"tidal": {"playlist_cache_refresh_secs": 0}}

    tpp = TidalPlaylistsProvider(backend)
    tpp._playlists = PlaylistCache(persist=False)
    yield tpp, backend


def test_create(tpp, mocker):
    tpp, backend = tpp
    playlist = mocker.Mock(last_updated=9, id="17")
    playlist.tracks_paginated.__name__ = "tracks"
    playlist.tracks_paginated.return_value = []
    playlist.name = "playlist name"
    backend.session.user.create_playlist.return_value = playlist
    p = tpp.create("playlist")
    assert p == MopidyPlaylist(
        last_modified=9, name="playlist name", uri="tidal:playlist:17"
    )
    backend.session.user.create_playlist.assert_called_once_with("playlist", "")


def test_delete(tpp):
    tpp, backend = tpp
    tpp.delete("tidal:playlist:19")
    backend.session.request.request.assert_called_once_with("DELETE", "playlists/19")


def test_delete_http_404(tpp, mocker):
    tpp, backend = tpp
    response = mocker.Mock(status_code=404)
    error = HTTPError()
    error.response = response
    backend.session.request.request.side_effect = error
    with pytest.raises(HTTPError) as e:
        tpp.delete("tidal:playlist:19")
        assert e.response == response
    backend.session.request.request.assert_called_once_with("DELETE", "playlists/19")


def test_delete_http_401_in_favourites(tpp, mocker):
    """
    Test removing from favourites.

    We should just remove the playlist from user favourites if its present but
    we get a 401 for deleting it.
    """
    tpp, backend = tpp
    session = backend.session
    response = mocker.Mock(status_code=401)
    error = HTTPError()
    error.response = response
    session.request.request.side_effect = error
    pl = mocker.Mock()
    pl.id = 21
    session.user.favorites.playlists_paginated.return_value = [pl]
    tpp.delete("tidal:playlist:21")
    session.user.favorites.remove_playlist.assert_called_once_with("21")
    session.request.request.assert_called_once_with("DELETE", "playlists/21")


def test_delete_http_401_not_in_favourites(tpp, mocker):
    """
    Test removing from favourites.

    We should just remove the playlist from user favourites if its present but
    we get a 401 for deleting it.
    """
    tpp, backend = tpp
    session = backend.session
    response = mocker.Mock(status_code=401)
    error = HTTPError()
    error.response = response
    session.request.request.side_effect = error
    pl = mocker.Mock()
    pl.id = 21
    session.user.favorites.playlists_paginated.return_value = [pl]
    with pytest.raises(HTTPError) as e:
        tpp.delete("tidal:playlist:678")
        assert e.response == response
    session.request.request.assert_called_once_with("DELETE", "playlists/678")


def test_save_no_changes(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    session = backend.session
    tidal_pl = tidal_playlists[0]
    uri = tidal_pl.uri
    mopidy_pl = mocker.Mock(
        uri=uri,
        last_modified=10,
        tracks=tidal_pl.tracks,
    )
    mopidy_pl.name = tidal_pl.name
    session.playlist.return_value = tidal_pl
    session.user.favorites.playlists_paginated.__name__ = "pl"
    session.user.favorites.playlists_paginated.return_value = [tidal_pl]
    session.user.playlists.return_value = []
    tpp._playlists[uri] = mopidy_pl
    tpp.save(mopidy_pl)
    assert tpp._playlists[uri] == mopidy_pl


def test_save_change_name(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    session = backend.session
    tidal_pl = tidal_playlists[0]
    uri = tidal_pl.uri
    mopidy_pl = mocker.Mock(
        uri=uri,
        last_modified=10,
        tracks=tidal_pl.tracks,
    )
    mopidy_pl.name = tidal_pl.name
    session.playlist.return_value = tidal_pl
    session.user.favorites.playlists_paginated.__name__ = "pl"
    session.user.favorites.playlists_paginated.return_value = [tidal_pl]
    session.user.playlists.return_value = []
    tpp._playlists[uri] = mopidy_pl
    pl = deepcopy(mopidy_pl)
    pl.name += "NEW"
    tpp.save(pl)
    session.playlist.assert_called_with("101")
    session.playlist().edit.assert_called_once_with(title=pl.name)


def test_save_remove(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    session = backend.session
    tidal_pl = tidal_playlists[0]
    uri = tidal_pl.uri
    mopidy_pl = mocker.Mock(
        uri=uri,
        last_modified=10,
        tracks=tidal_pl.tracks,
    )
    mopidy_pl.name = tidal_pl.name
    session.playlist.return_value = tidal_pl
    session.user.favorites.playlists_paginated.__name__ = "pl"
    session.user.favorites.playlists_paginated.return_value = [tidal_pl]
    session.user.playlists.return_value = []
    tpp._playlists[uri] = mopidy_pl
    pl = deepcopy(mopidy_pl)
    pl.tracks = pl.tracks[:1]
    tpp.save(pl)
    session.playlist.assert_called_with("101")
    session.playlist().remove_by_index.assert_called_once_with(1)


def test_save_add(tpp, mocker, tidal_playlists, tidal_tracks):
    tpp, backend = tpp
    session = backend.session
    tidal_pl = tidal_playlists[0]
    uri = tidal_pl.uri
    mopidy_pl = mocker.Mock(
        uri=uri,
        last_modified=10,
        tracks=tidal_pl.tracks,
    )
    mopidy_pl.name = tidal_pl.name
    session.playlist.return_value = tidal_pl
    session.user.favorites.playlists_paginated.__name__ = "pl"
    session.user.favorites.playlists_paginated.return_value = [tidal_pl]
    session.user.playlists.return_value = []
    tpp._playlists[uri] = mopidy_pl
    pl = deepcopy(mopidy_pl)
    pl.tracks += tidal_tracks[-2:-1]
    tpp.save(pl)
    session.playlist.assert_called_with("101")
    session.playlist().add.assert_called_once_with(["0"])


def test_lookup_unmodified_cached(tpp, mocker):
    tpp, backend = tpp
    remote_playlist = mocker.Mock(last_updated=9)
    backend.session.playlist.return_value = remote_playlist
    playlist = mocker.MagicMock(last_modified=9)
    tpp._playlists["tidal:playlist:0:1:2"] = playlist
    assert tpp.lookup("tidal:playlist:0:1:2") is playlist


def test_refresh_metadata(tpp, mocker, tidal_playlists):
    listener = mocker.Mock()
    mocker.patch("mopidy_tidal.playlists.backend.BackendListener", listener)
    tpp, backend = tpp
    tpp._current_tidal_playlists = tidal_playlists
    assert not len(tpp._playlists_metadata)
    tpp.refresh(include_items=False)

    listener.send.assert_called_once_with("playlists_loaded")

    tracks = [Track(uri="tidal:track:0:0:0")] * 2
    assert dict(tpp._playlists_metadata) == {
        "tidal:playlist:101": MopidyPlaylist(
            last_modified=10,
            name="Playlist-101",
            uri="tidal:playlist:101",
            tracks=tracks,
        ),
        "tidal:playlist:222": MopidyPlaylist(
            last_modified=10,
            name="Playlist-222",
            uri="tidal:playlist:222",
            tracks=tracks[:1],
        ),
    }


def api_test(tpp, mocker, api_method, tp):
    listener = mocker.Mock()
    mocker.patch("mopidy_tidal.playlists.backend.BackendListener", listener)
    tpp._current_tidal_playlists = [tp]
    tracks = [mocker.Mock() for _ in range(2)]
    for i, track in enumerate(tracks):
        track.id = i
        track.uri = f"tidal:track:{i}:{i}:{i}"
        track.name = f"Track-{i}"
        track.full_name = f"{track.name} (version)"
        track.artist.name = "artist_name"
        track.artist.id = i
        track.album.name = "album_name"
        track.album.id = i
        track.duration = 100 + i
        track.track_num = i
        track.disc_num = i
    api_method.return_value = tracks
    api_method.__name__ = "get_playlist_tracks"

    tpp.refresh(include_items=True)
    listener.send.assert_called_once_with("playlists_loaded")
    assert len(tpp._playlists) == 1
    playlist = tpp._playlists["tidal:playlist:1-1-1"]
    assert isinstance(playlist, MopidyPlaylist)
    assert playlist.last_modified == 10
    assert playlist.name == "Playlist-1"
    assert playlist.uri == "tidal:playlist:1-1-1"
    assert len(playlist.tracks) == 2 * len(api_method.mock_calls)
    attr_map = {"disc_num": "disc_no", "full_name": "name"}
    assert all(
        getattr(orig_tr, k) == getattr(tr, attr_map.get(k, k))
        for orig_tr, tr in zip(tracks, playlist.tracks)
        for k in {"uri", "disc_num", "full_name"}
    )


@pytest.mark.xfail(reason="Broken test due to change in API")
def test_refresh(tpp, mocker):
    tpp, backend = tpp
    session = backend.session
    session.mock_add_spec([])
    tp = mocker.Mock(spec=TidalPlaylist, session=mocker.Mock, playlist_id="1-1-1")
    tp.id = tp.playlist_id
    tp.name = "Playlist-1"
    tp.last_updated = 10
    tp.tracks = mocker.Mock()
    api_method = tp.tracks
    api_test(tpp, mocker, api_method, tp)


def test_as_list(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    mocker.patch("tidalapi.workers.get_items", lambda x: x)
    # Mock the single playlists_paginated method returning all playlists
    backend.session.user.favorites.playlists_paginated.return_value = tidal_playlists
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
        Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
    ]


def test_prevent_duplicate_playlist_sync(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    mocker.patch("tidalapi.workers.get_items", lambda x: x)

    # Mock the single playlists_paginated method returning all playlists
    backend.session.user.favorites.playlists_paginated.return_value = tidal_playlists

    # First load
    tpp.as_list()

    # Create a new mock playlist (p) to simulate a new playlist
    p = mocker.Mock(spec=TidalPlaylist, session=mocker.Mock, playlist_id="2-2-2")

    # Add it to the playlists_paginated return list
    updated_playlists = tidal_playlists + [p]
    backend.session.user.favorites.playlists_paginated.return_value = updated_playlists

    # Verify no duplicates or issues on second load
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
        Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
    ]


def test_playlist_sync_downtime(mocker, tidal_playlists, config):
    backend = mocker.Mock()
    tpp = TidalPlaylistsProvider(backend)
    tpp._playlists = PlaylistCache(persist=False)
    mocker.patch("tidalapi.workers.get_items", lambda x: x)
    backend._config = {"tidal": {"playlist_cache_refresh_secs": 0.1}}

    # Mock playlists_paginated() returning the first playlist initially
    backend.session.user.favorites.playlists_paginated.return_value = tidal_playlists[
        :1
    ]

    # Trigger initial load
    tpp.as_list()
    # Create a new playlist mock object (p)
    p = mocker.Mock(spec=TidalPlaylist, session=mocker.Mock, playlist_id="2")
    p.id = p.playlist_id
    p.num_tracks = 2
    p.name = "Playlist-2"
    p.last_updated = 10

    # Update the return_value of playlists_paginated to include the new playlist p plus the rest
    updated_playlists = tidal_playlists[:1] + [p] + tidal_playlists[1:]
    backend.session.user.favorites.playlists_paginated.return_value = updated_playlists

    # First assertion after initial load
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
    ]
    sleep(0.1)
    # After “downtime” simulate refreshed playlists with the new playlist added
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
        Ref(name="Playlist-2", type="playlist", uri="tidal:playlist:2"),
        Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
    ]


def test_update_changes(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    tpp._playlists_metadata.update(
        {
            "tidal:playlist:101": MopidyPlaylist(
                last_modified=10, name="Playlist-101", uri="tidal:playlist:101"
            ),
            "tidal:playlist:222": MopidyPlaylist(
                last_modified=9, name="Playlist-222", uri="tidal:playlist:222"
            ),
        }
    )

    mocker.patch("tidalapi.workers.get_items", lambda x: x)
    backend.session.user.favorites.playlists_paginated.return_value = tidal_playlists
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
        Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
    ]


def test_update_no_changes(tpp, mocker, tidal_playlists):
    tpp, backend = tpp
    tpp._playlists_metadata.update(
        {
            "tidal:playlist:101": MopidyPlaylist(
                last_modified=10, name="Playlist-101", uri="tidal:playlist:101"
            ),
            "tidal:playlist:222": MopidyPlaylist(
                last_modified=10, name="Playlist-222", uri="tidal:playlist:222"
            ),
        }
    )

    mocker.patch("tidalapi.workers.get_items", lambda x: x)
    backend.session.user.favorites.playlists_paginated.return_value = tidal_playlists
    assert tpp.as_list() == [
        Ref(name="Playlist-101", type="playlist", uri="tidal:playlist:101"),
        Ref(name="Playlist-222", type="playlist", uri="tidal:playlist:222"),
    ]


def test_lookup_modified_cached(tpp, mocker):
    tpp, backend = tpp
    remote_playlist = mocker.Mock(last_updated=10)
    backend.session.playlist.return_value = remote_playlist
    playlist = mocker.MagicMock(last_modified=9)
    tpp._playlists["tidal:playlist:0:1:2"] = playlist
    assert tpp.lookup("tidal:playlist:0:1:2") is playlist


def test_get_items_none(tpp):
    tpp, backend = tpp
    assert not tpp.get_items("tidal:playlist:0-1-2")


def test_get_items_none_upstream(tpp, mocker):
    tpp, backend = tpp
    backend.session.playlist.return_value = None
    tracks = [mocker.Mock() for _ in range(2)]
    for i, track in enumerate(tracks):
        track.uri = f"tidal:track:{i}:{i}:{i}"
        track.name = f"Track-{i}"

    playlist = mocker.MagicMock(last_modified=9, tracks=tracks)
    tpp._playlists["tidal:playlist:0-1-2"] = playlist
    assert tpp.get_items("tidal:playlist:0-1-2") == [
        Ref(name="Track-0", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1", type="track", uri="tidal:track:1:1:1"),
    ]


def test_get_items_playlists(tpp, mocker):
    tpp, backend = tpp
    backend.session.playlist.return_value = mocker.Mock(last_updated=9)
    tracks = [mocker.Mock() for _ in range(2)]
    for i, track in enumerate(tracks):
        track.uri = f"tidal:track:{i}:{i}:{i}"
        track.name = f"Track-{i}"

    playlist = mocker.MagicMock(last_modified=9, tracks=tracks)
    tpp._playlists["tidal:playlist:0-1-2"] = playlist
    assert tpp.get_items("tidal:playlist:0-1-2") == [
        Ref(name="Track-0", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1", type="track", uri="tidal:track:1:1:1"),
    ]


def test_get_items_playlists_no_updated(tpp, mocker):
    tpp, backend = tpp
    backend.session.playlist.return_value = mocker.Mock(spec={})
    tracks = [mocker.Mock() for _ in range(2)]
    for i, track in enumerate(tracks):
        track.uri = f"tidal:track:{i}:{i}:{i}"
        track.name = f"Track-{i}"

    playlist = mocker.MagicMock(last_modified=9, tracks=tracks)
    tpp._playlists["tidal:playlist:0-1-2"] = playlist
    assert tpp.get_items("tidal:playlist:0-1-2") == [
        Ref(name="Track-0", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1", type="track", uri="tidal:track:1:1:1"),
    ]


@pytest.mark.xfail(reason="Broken test due to change in API")
def test_get_items_mix(tpp, mocker):
    tpp, backend = tpp
    tracks = [mocker.Mock() for _ in range(2)]
    for i, track in enumerate(tracks):
        track.id = i
        track.uri = f"tidal:track:{i}:{i}:{i}"
        track.name = f"Track-{i}"
        track.full_name = f"{track.name} (version)"
        track.artist.name = "artist_name"
        track.artist.id = i
        track.album.name = "album_name"
        track.album.id = i
        track.duration = 100
        track.track_num = i
        track.disc_num = i
    tidal_playlist = mocker.Mock(last_updated=9)
    tidal_playlist.items.return_value = tracks
    backend.session.mix.return_value = tidal_playlist

    playlist = mocker.MagicMock(last_modified=9, tracks=tracks)
    tpp._playlists["tidal:mix:0-1-2"] = playlist
    assert tpp.get_items("tidal:mix:0-1-2") == [
        Ref(name="Track-0 (version)", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1 (version)", type="track", uri="tidal:track:1:1:1"),
    ]
