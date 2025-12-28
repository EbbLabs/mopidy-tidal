import pytest
from mopidy.models import Album, Artist, Image, Ref, SearchResult, Track
from requests import HTTPError
from tidalapi.playlist import Playlist

from mopidy_tidal.library import HTTPError, ObjectNotFound, TidalLibraryProvider


@pytest.fixture
def library_provider(backend, config):
    lp = TidalLibraryProvider(backend)
    for cache_type in {"artist", "album", "track", "playlist"}:
        getattr(lp, f"_{cache_type}_cache")._persist = False

    return lp


def test_get_track_images(library_provider, backend, mocker):
    uris = ["tidal:track:0-0-0:1-1-1:2-2-2"]
    get_album = mocker.Mock()
    get_album.image.return_value = "tidal:album:1-1-1"
    backend.session.album.return_value = get_album
    assert library_provider.get_images(uris) == {
        uris[0]: [Image(height=320, uri="tidal:album:1-1-1", width=320)]
    }
    backend.session.album.assert_called_once_with("1-1-1")


@pytest.mark.xfail
def test_track_cache(library_provider, backend, mocker):
    # I think the caching logic is broken here
    uris = ["tidal:track:0-0-0:1-1-1:2-2-2"]
    get_album = mocker.Mock()
    get_album.image.return_value = "tidal:album:1-1-1"
    backend.session.album.return_value = get_album
    first = library_provider.get_images(uris)
    assert first == {uris[0]: [Image(height=320, uri="tidal:album:1-1-1", width=320)]}
    assert library_provider.get_images(uris) == first
    backend.session.album.assert_called_once_with("1-1-1")


@pytest.mark.xfail(reason="returning nothing")
def test_get_noimages(library_provider, backend):
    uris = ["tidal:track:0-0-0:1-1-1:2-2-2"]
    backend.session.mock_add_spec([])
    assert library_provider.get_images(uris) == {uris[0]: []}


class TestSearch:
    def test_defers_to_tidal_search(self, library_provider, mocker):
        artists = [mocker.Mock(spec=Artist)]
        albums = [mocker.Mock(spec=Album)]
        tracks = [mocker.Mock(spec=Track)]
        tidal_search = mocker.patch(
            "mopidy_tidal.search.tidal_search", return_value=(artists, albums, tracks)
        )

        result = library_provider.search(query="songs", exact=False)

        assert result == SearchResult(artists=artists, albums=albums, tracks=tracks)
        tidal_search.assert_called_once()
        assert tidal_search.mock_calls[0].kwargs["query"] == "songs"
        assert tidal_search.mock_calls[0].kwargs["exact"] is False

    def test_returns_none_if_no_match(self, library_provider, mocker):
        tidal_search = mocker.patch(
            "mopidy_tidal.search.tidal_search", return_value=None
        )

        assert not library_provider.search("nonsuch")

        tidal_search.assert_called_once()
        assert tidal_search.mock_calls[0].kwargs["query"] == "nonsuch"


class TestBrowse:
    def test_invalid_uri_returns_empty_list(self, library_provider):
        assert library_provider.browse("") == []
        assert library_provider.browse("spotify:something:something_else") == []
        assert (
            library_provider.browse("tidal:album:oneid:onemoreid:oneidtoomany:") == []
        )

    def test_root_uri_returns_all_options_as_refs(self, library_provider):
        assert library_provider.browse("tidal:directory") == [
            Ref(name="Home", type="directory", uri="tidal:home"),
            Ref(name="For You", type="directory", uri="tidal:for_you"),
            Ref(name="Explore", type="directory", uri="tidal:explore"),
            Ref(name="HiRes", type="directory", uri="tidal:hires"),
            Ref(name="Genres", type="directory", uri="tidal:genres"),
            Ref(name="Moods", type="directory", uri="tidal:moods"),
            Ref(name="My Mixes", type="directory", uri="tidal:mixes"),
            Ref(name="My Artists", type="directory", uri="tidal:my_artists"),
            Ref(name="My Albums", type="directory", uri="tidal:my_albums"),
            Ref(name="My Playlists", type="directory", uri="tidal:my_playlists"),
            Ref(name="My Tracks", type="directory", uri="tidal:my_tracks"),
            Ref(name="Mixes & Radio", type="directory", uri="tidal:my_mixes"),
        ]

    def test_my_artists_returns_favourite_artists_from_tidal_as_refs(
        self, library_provider, session, mocker, make_tidal_artist
    ):
        session.user.favorites.artists_paginated.return_value = [
            make_tidal_artist(name="Arty", id=1),
            make_tidal_artist(name="Arthur", id=1_000),
        ]
        mocker.patch("tidalapi.workers.get_items", lambda x: x)

        assert library_provider.browse("tidal:my_artists") == [
            Ref(name="Arty", type="artist", uri="tidal:artist:1"),
            Ref(name="Arthur", type="artist", uri="tidal:artist:1000"),
        ]

    def test_my_albums_returns_favourite_albums_from_tidal_as_refs(
        self, library_provider, session, mocker, make_tidal_album
    ):
        session.user.favorites.albums_paginated.return_value = [
            make_tidal_album(name="Alby", id=7),
            make_tidal_album(name="Albion", id=7_000),
        ]
        mocker.patch("tidalapi.workers.get_items", lambda x: x)

        assert library_provider.browse("tidal:my_albums") == [
            Ref(name="Alby", type="album", uri="tidal:album:7"),
            Ref(name="Albion", type="album", uri="tidal:album:7000"),
        ]

    def test_my_tracks_returns_favourite_tracks_from_tidal_as_refs(
        self,
        library_provider,
        session,
        mocker,
        make_tidal_track,
        make_tidal_album,
        make_tidal_artist,
    ):
        artist = make_tidal_artist(name="Arty", id=6)
        album = make_tidal_album(name="Albion", id=7)
        session.user.favorites.tracks_paginated.return_value = [
            make_tidal_track(name="Tracky", id=12, artist=artist, album=album),
            make_tidal_track(name="Traction", id=13, artist=artist, album=album),
        ]
        mocker.patch("tidalapi.workers.get_items", lambda x: x)

        assert library_provider.browse("tidal:my_tracks") == [
            Ref(name="Tracky", type="track", uri="tidal:track:6:7:12"),
            Ref(name="Traction", type="track", uri="tidal:track:6:7:13"),
        ]

    @pytest.mark.insufficiently_decoupled
    def test_my_playlists_defers_to_backend_as_list(
        self, library_provider, backend, mocker
    ):
        """backend.as_list is quite complicated, so we test it separately.

        This test asserts that our test of backend_as_list covers the code.
        But it's not a good test all the same as it's tied to our implementation.
        """
        uniq = object()
        as_list = mocker.Mock(return_value=uniq)
        backend.playlists.as_list = as_list

        assert library_provider.browse("tidal:my_playlists") is uniq

        as_list.assert_called_once_with()

    def test_moods_returns_moods_from_tidal_as_refs(
        self, library_provider, session, make_tidal_page
    ):
        mood = make_tidal_page(title="Moody", categories=[], api_path="0/0/18")
        session.moods.return_value = [mood]

        result = library_provider.browse("tidal:moods")

        assert result == [Ref(name="Moody", type="directory", uri="tidal:mood:18")]
        session.moods.assert_called_once_with()

    def test_mixes_returns_mixes_from_tidal_as_refs(
        self, library_provider, session, make_tidal_mix
    ):
        session.mixes.return_value = [
            make_tidal_mix(title="Mick's mix", sub_title="Micky mouse", id=19_678),
            make_tidal_mix(title="Micky", sub_title="Mouse", id=6),
        ]

        assert library_provider.browse("tidal:mixes") == [
            Ref(
                name="Mick's mix (Micky mouse)", type="playlist", uri="tidal:mix:19678"
            ),
            Ref(name="Micky (Mouse)", type="playlist", uri="tidal:mix:6"),
        ]
        session.mixes.assert_called_once_with()

    def test_genres_returns_genres_from_tidal_as_refs(
        self, library_provider, session, make_tidal_genre
    ):
        session.genre.get_genres.return_value = [
            make_tidal_genre(name="Jean Re", path="12"),
            make_tidal_genre(name="John Ra", path="1345"),
        ]

        assert library_provider.browse("tidal:genres") == [
            Ref(name="Jean Re", type="directory", uri="tidal:genre:12"),
            Ref(name="John Ra", type="directory", uri="tidal:genre:1345"),
        ]
        session.genre.get_genres.assert_called_once_with()


class TestBrowseAlbum:
    def test_missing_album_returns_empty_list(self, library_provider, session):
        session.album.side_effect = HTTPError("No such album")

        assert library_provider.browse("tidal:album:1") == []
        session.album.assert_called_once_with("1")

    def test_album_returns_tracks(
        self,
        library_provider,
        session,
        make_tidal_album,
        make_tidal_artist,
    ):
        artist = make_tidal_artist(name="Arty", id=789)
        album = make_tidal_album(
            name="Alby",
            id=17,
            tracks=[
                dict(name="Traction", id=17, artist=artist),
                dict(name="Tracky", id=65, artist=artist),
            ],
        )
        session.album.return_value = album

        assert library_provider.browse("tidal:album:1") == [
            Ref(name="Traction", type="track", uri="tidal:track:789:17:17"),
            Ref(name="Tracky", type="track", uri="tidal:track:789:17:65"),
        ]


class TestGetImages:
    def test_track_uri_resolves_to_album_images(
        self, library_provider, session, mocker
    ):
        session.album.return_value.image.return_value = "tidal:album:1-1-1"

        images = library_provider.get_images(["tidal:track:0-0-0:1-1-1:2-2-2"])

        assert images == {
            "tidal:track:0-0-0:1-1-1:2-2-2": [
                Image(height=320, uri="tidal:album:1-1-1", width=320)
            ]
        }
        session.album.assert_called_once_with("1-1-1")


class TestGetDistinct:
    @pytest.mark.parametrize("field", ("artist", "album", "track"))
    def test_returns_all_favourites_with_watermark_when_no_query_given(
        self, library_provider, session, field, make_mock
    ):
        titles = [make_mock(name=f"Title-{i}") for i in range(2)]
        session.configure_mock(**{f"user.favorites.{field}s.return_value": titles})

        res = library_provider.get_distinct(field)

        assert len(res) == 2
        assert res == {"Title-0 [TIDAL]", "Title-1 [TIDAL]"}

    def test_returns_empty_set_when_no_match(self, library_provider):
        assert library_provider.get_distinct("nonsuch") == set()

    def test_returns_empty_set_when_no_match_with_query(self, library_provider):
        assert library_provider.get_distinct("nonsuch", query={"any": "any"}) == set()

    @pytest.mark.parametrize("field", ("artist", "track"))
    def test_query_ignored_when_field_is_not_album_or_albumartist(  # TODO why do we do this?
        self, library_provider, session, field, make_mock
    ):
        titles = [make_mock(name=f"Title-{i}") for i in range(2)]
        session.configure_mock(**{f"user.favorites.{field}s.return_value": titles})

        res = library_provider.get_distinct(field, query={"any": "any"})

        assert len(res) == 2
        assert res == {"Title-0 [TIDAL]", "Title-1 [TIDAL]"}

    @pytest.mark.parametrize("field", ("album", "albumartist"))
    def test_get_distinct_returns_empty_set_when_search_returns_no_results(
        self, library_provider, session, mocker, field
    ):
        tidal_search = mocker.Mock(return_value=([], [], []))
        mocker.patch("mopidy_tidal.search.tidal_search", tidal_search)

        res = library_provider.get_distinct(field, query={"any": "any"})

        assert res == set()
        tidal_search.assert_called_once_with(session, query={"any": "any"}, exact=True)

    @pytest.mark.parametrize("field", ("album", "albumartist"))
    def test_get_distinct_returns_search_results_with_watermark(
        self, library_provider, session, mocker, field, make_mock
    ):
        arty_albums = [make_mock(name=f"Arty Album {i}") for i in range(2)]
        artist = make_mock(
            mock=mocker.Mock(**{"get_albums.return_value": arty_albums}),
            name="Arty",
            uri="tidal:artist:1",
        )
        tidal_search = mocker.Mock(return_value=([artist], [], []))
        session.artist.return_value = artist
        mocker.patch("mopidy_tidal.search.tidal_search", tidal_search)

        res = library_provider.get_distinct(field, query={"any": "any"})

        tidal_search.assert_called_once_with(session, query={"any": "any"}, exact=True)
        session.artist.assert_called_once_with("1")
        artist.get_albums.assert_called_once_with()
        assert len(res) == 2
        assert res == {"Arty Album 0 [TIDAL]", "Arty Album 1 [TIDAL]"}

    @pytest.mark.parametrize("field", ("album", "albumartist"))
    def test_get_distinct_returns_empty_set_when_artist_not_found(
        self, library_provider, session, mocker, field, make_mock
    ):
        artist = make_mock(name="Arty", uri="tidal:artist:1")
        tidal_search = mocker.Mock(return_value=([artist], [], []))
        mocker.patch("mopidy_tidal.search.tidal_search", tidal_search)
        session.artist.side_effect = (
            ObjectNotFound  # looking up artist will result in ObjectNotFound
        )

        assert library_provider.get_distinct(field, query={"any": "any"}) == set()
        tidal_search.assert_called_once_with(session, query={"any": "any"}, exact=True)
        session.artist.assert_called_once_with("1")


class TestLookup:
    def test_raises_when_no_uri_passed(self, library_provider):
        with pytest.raises(Exception):
            library_provider.lookup()

    @pytest.mark.parametrize("uri", ("", "this_isn't_a_uri"))
    def test_raises_with_invalid_uri(self, library_provider, uri):
        with pytest.raises(Exception):
            library_provider.lookup(uri)

    def test_returns_empty_list_if_http_request_fails(
        self, library_provider, session, mocker
    ):
        album = mocker.Mock(**{"tracks.side_effect": HTTPError})
        session.album.return_value = album

        assert library_provider.lookup("tidal:track:0:1:0") == []


def test_specific_playlist(library_provider, backend, mocker, tidal_tracks):
    session = backend.session
    session.mock_add_spec(("playlist",))
    playlist = mocker.Mock(name="Playlist")
    playlist.tracks_paginated.return_value = tidal_tracks
    playlist.tracks_paginated.__name__ = "playlist"
    session.playlist.return_value = playlist

    tracks = library_provider.browse("tidal:playlist:1")
    assert tracks[:2] == [
        Ref(name="Track-0", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1", type="track", uri="tidal:track:1:1:1"),
    ]

    session.playlist.assert_called_once_with("1")


def test_specific_mood(library_provider, backend, mocker):
    session = backend.session
    session.mock_add_spec(("moods",))
    playlist = mocker.Mock()
    playlist.id = 0
    playlist.name = "Playlist-0"
    mood = mocker.Mock()
    mood.id = 1
    mood.name = "Mood-1"
    mood.api_path = "something/somethingelse/1"
    mood.get.return_value = [playlist]
    mood_2 = mocker.Mock()
    mood_2.api_path = "0/0/0"
    session.moods.return_value = [mood, mood_2]
    assert library_provider.browse("tidal:mood:1") == [
        Ref(name="Playlist-0", type="playlist", uri="tidal:playlist:0"),
    ]

    session.moods.assert_called_once_with()
    mood.get.assert_called_once_with()


def test_specific_mood_none(library_provider, backend, mocker, tidal_tracks):
    session = backend.session
    session.mock_add_spec(("moods",))
    playlist_2 = mocker.Mock()
    playlist_2.api_path = "0/0/0"
    session.moods.return_value = [playlist_2]
    assert not library_provider.browse("tidal:mood:1")


def test_specific_genre(library_provider, backend, mocker):
    session = backend.session
    session.mock_add_spec(("genre", "genre.get_genres"))
    playlist = mocker.Mock()
    playlist.id = 0
    playlist.name = "Playlist-0"
    genre = mocker.Mock()
    genre.id = 1
    genre.name = "Genre-1"
    genre.path = "1"
    genre.items.return_value = [playlist]
    genre_2 = mocker.Mock()
    genre_2.path = "13"
    session.genre.get_genres.return_value = [genre, genre_2]
    assert library_provider.browse("tidal:genre:1") == [
        Ref(name="Playlist-0", type="playlist", uri="tidal:playlist:0"),
    ]
    session.genre.get_genres.assert_called_once_with()
    genre.items.assert_called_once_with(Playlist)


def test_specific_genre_none(library_provider, backend, mocker, tidal_tracks):
    session = backend.session
    session.mock_add_spec(("genre", "genre.get_genres"))
    playlist_2 = mocker.Mock()
    playlist_2.path = "13"
    session.genre.get_genres.return_value = [playlist_2]
    assert not library_provider.browse("tidal:genre:1")
    session.genre.get_genres.assert_called_once_with()


def test_specific_mix(library_provider, backend, mocker, tidal_tracks):
    session = backend.session
    playlist = mocker.Mock()
    playlist.id = "1"
    playlist.name = "Playlist-1"
    playlist.items.return_value = tidal_tracks
    session.mix.return_value = playlist
    assert library_provider.browse("tidal:mix:1") == [
        Ref(name="Track-0", type="track", uri="tidal:track:0:0:0"),
        Ref(name="Track-1", type="track", uri="tidal:track:1:1:1"),
    ]
    playlist.items.assert_called_once_with()


def test_specific_mix_none(library_provider, backend, mocker):
    """
    Nonexistent mix should throw ObjectNotFound.
    Note: mix.items() is never called in this case
    """
    session = backend.session
    session.mix.side_effect = Exception("ObjectNotFound")
    with pytest.raises(Exception, match="ObjectNotFound"):
        library_provider.browse("tidal:mix:1")
    session.mix.items.assert_not_called()


def test_specific_artist(
    library_provider, backend, mocker, tidal_albums, tidal_artists
):
    session = backend.session
    session.mock_add_spec(("artist",))
    artist = tidal_artists[0]
    artist.get_albums.return_value = tidal_albums
    session.artist.return_value = artist
    assert library_provider.browse("tidal:artist:1") == [
        Ref(name="Album-0", type="album", uri="tidal:album:0"),
        Ref(name="Album-1", type="album", uri="tidal:album:1"),
        Ref(name="Track-100", type="track", uri="tidal:track:0:7:100"),
    ]
    artist.get_top_tracks.assert_called_once_with()
    artist.get_albums.assert_called_once_with()
    session.artist.assert_has_calls([mocker.call("1"), mocker.call("1")])


def test_lookup_track(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    res = library_provider.lookup("tidal:track:0:1:0")
    compare(tidal_tracks[:1], res, "track")
    session.album.assert_called_once_with("1")


def test_lookup_track_newstyle(
    library_provider, backend, mocker, tidal_tracks, compare
):
    session = backend.session
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    track = mocker.Mock(**{"album.id": 1, "name": "track"})
    session.track.return_value = track
    res = library_provider.lookup("tidal:track:0")
    compare(tidal_tracks[:1], res, "track")
    session.album.assert_called_once_with("1")
    session.track.assert_called_once_with("0")


def test_lookup_track_cached(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    res = library_provider.lookup("tidal:track:0:1:0")
    compare(tidal_tracks[:1], res, "track")
    res2 = library_provider.lookup("tidal:track:0:1:0")
    assert res2 == res
    session.album.assert_called_once_with("1")


def test_lookup_track_cached_album(
    library_provider, backend, mocker, tidal_albums, compare
):
    session = backend.session
    tidal_tracks = tidal_albums[1].tracks()
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    res = library_provider.lookup("tidal:album:1")
    compare(tidal_tracks, res, "track")
    res2 = library_provider.lookup("tidal:track:1234:1:1")
    compare(tidal_tracks[:1], res2, "track")
    session.album.assert_called_once_with("1")


def test_lookup_album(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    res = library_provider.lookup("tidal:album:1")
    compare(tidal_tracks, res, "track")
    session.album.assert_called_once_with("1")


def test_lookup_album_cached(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    album = mocker.Mock()
    album.tracks.return_value = tidal_tracks
    session.album.return_value = album
    res = library_provider.lookup("tidal:album:1")
    compare(tidal_tracks, res, "track")
    res2 = library_provider.lookup("tidal:album:1")
    assert res2 == res
    session.album.assert_called_once_with("1")


def test_lookup_artist(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    artist = mocker.Mock()
    artist.get_top_tracks.return_value = tidal_tracks
    session.artist.return_value = artist
    res = library_provider.lookup("tidal:artist:1")
    compare(tidal_tracks, res, "track")
    session.artist.assert_called_once_with("1")


def test_lookup_artist_cached(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    artist = mocker.Mock()
    artist.get_top_tracks.return_value = tidal_tracks
    session.artist.return_value = artist
    res = library_provider.lookup("tidal:artist:1")
    compare(tidal_tracks, res, "track")
    res2 = library_provider.lookup("tidal:artist:1")
    assert res2 == res
    session.artist.assert_called_once_with("1")


def test_lookup_playlist(library_provider, backend, mocker, tidal_tracks, compare):
    session = backend.session
    playlist = mocker.Mock()
    playlist.name = "Playlist-1"
    playlist.last_updated = 10
    session.playlist.return_value = playlist
    playlist.tracks_paginated.return_value = tidal_tracks
    playlist.tracks_paginated.__name__ = "get_playlist_tracks"

    res = library_provider.lookup("tidal:playlist:99")
    compare(tidal_tracks, res[: len(tidal_tracks)], "track")

    session.playlist.assert_called_with("99")


def test_lookup_playlist_cached(
    library_provider, backend, mocker, tidal_tracks, compare
):
    session = backend.session
    playlist = mocker.Mock()
    playlist.name = "Playlist-1"
    playlist.last_updated = 10
    session.playlist.return_value = playlist
    playlist.tracks_paginated.return_value = tidal_tracks
    playlist.tracks_paginated.__name__ = "get_playlist_tracks"

    res = library_provider.lookup("tidal:playlist:99")
    compare(tidal_tracks, res[: len(tidal_tracks)], "track")
    res2 = library_provider.lookup("tidal:playlist:99")
    assert res2 == res

    session.playlist.assert_called_with("99")
