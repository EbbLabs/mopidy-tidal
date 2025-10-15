# Changelog

#### v0.3.12

- tidalapi version bump to v0.8.7

#### v0.3.11

- Bugfix: Set playlist fetch limit to 50 items due to v2 endpoint limitations
- Feature: Use tidalapi pagination for album, artist, track and playlist and playlist tracks.
- Use legacy home page as fallback until V2 is supported.
- Bugfix: Return all artists for tracks and albums. (Fixes: #189)
- tidalapi version bump to v0.8.6

#### v0.3.10

- Don't break tracks import if the track_id is not found
- feature(library): Support Mopidy v4.0 tracks

(Thanks [BlackLight](https://github.com/BlackLight) for the above PRs)

#### v0.3.9

- Bugfix: Handle get_urls() returning list in tidalapi v0.8.1

#### v0.3.8

- tidalapi version bump to v0.8.1
- Bugfix: Reverted auto-login only when AUTO enabled.

#### v0.3.7

- tidalapi version bump to v0.8.0
- Tests: Fixed unit tests

#### v0.3.6

- Bugfix: Fix missing images on tracks
- Readme: PKCE logon details added
- Print Mopidy-Tidal, Python-Tidal version info to log

#### v0.3.5

- Fix HI_RES_LOSSLESS playback with PKCE authentication
- Added support for two stage PKCE authentication using HTTP web server
- Add new categories (HiRes, Mixes & Radio, My Mixes)
- Add helper functions (create_category_directories) for navigating sub-categories
- Switch to using Stream MPEG-DASH MPD manifest directly for playback instead of direct URL
- Refactor/cleanup backend, move load/save session to tidalapi.
- Handle missing objects (ObjectNotFound) gracefully
- Handle HTTP 429 (TooManyRequests) gracefully
- Add auth_mode, login_server_port config params
- Add HI_RES (MQA), add auth_method, login_server_port, AUTO login_method as alias
- Fix missing pictures on some playlist types
- Skip video_mixes when generating mix playlists
- Rewrite test suite

  (Major thanks to [2e0byo](https://github.com/2e0byo) for test suite
  improvements, [quodrum-glas](https://github.com/quodrum-glas) for inspiration to use HTTP Server for PKCE
  authentication)

#### v0.3.4

- Added support for navigating For You, Explore pages.

#### v0.3.3

- Added HI_RES_LOSSLESS quality (Requires HiFi+ subscription)

#### v0.3.2

- Implemented a configurable `playlist_cache_refresh_secs`
- Replace colons in cache filenames with hyphens to add FAT32/NTFS compatibility

(Thanks [BlackLight](https://github.com/BlackLight) for the above PRs)

#### v0.3.1

- Added support for tidalapi 0.7.x. Tidalapi >=0.7.x is now required.
- Added support for Moods, Mixes, track/album release date.
- Speed, cache improvements and Iris bugfixes.
- Overhauled Test suite
- Support for playlist editing

(Major thanks [BlackLight](https://github.com/BlackLight) and [2e0byo](https://github.com/2e0byo) for the above
improvements and all testers involved)

#### v0.2.8

- Major caching improvements to avoid slow intialization at startup. Code cleanup, bugfixes and refactoring (
  Thanks [BlackLight](https://github.com/BlackLight), [fmarzocca](https://github.com/fmarzocca))
- Reduced default album art, author and track image size.
- Improved Iris integration

#### v0.2.7

- Use path extension for Tidal OAuth cache file
- Improved error handling for missing images, unplayable albums
- Improved playlist browsing

#### v0.2.6

- Improved reliability of OAuth cache file generation.
- Added optional client_id & client_secret to [tidal] in mopidy config (thanks Glog78)
- Removed username/pass, as it is not needed by OAuth (thanks tbjep)

#### v0.2.5

- Reload existing OAuth session on Mopidy restart
- Added OAuth login support from tidalapi (thanks to greggilbert)

#### v0.2.4

- Added track caching (thanks to MrSurly and kingosticks)

#### v0.2.3

- fixed python 3 compatibility issues
- Change dependency tidalapi4mopidy back to tidalapi (thanks to stevedenman)

#### v0.2.2

- added support browsing of favourite tracks, moods, genres and playlists (thanks to SERVCUBED)

#### v0.2.1

- implemented get_images method
- updated tidal's api key

#### v0.2.0

- playlist support (read-only)
- implemented artists lookup
- high and low quality streams should now work correctly
- cache search results (to be improved in next releases)

#### v0.1.0

- Initial release.
