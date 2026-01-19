# Mopidy-Tidal

[![Latest PyPI version](https://img.shields.io/pypi/v/Mopidy-Tidal.svg?style=flat)](https://github.com/tehkillerbee/mopidy-tidal)
[![Number of PyPI downloads](https://img.shields.io/pypi/dm/Mopidy-Tidal.svg?style=flat)](https://github.com/tehkillerbee/mopidy-tidal)
[![codecov](https://codecov.io/gh/tehkillerbee/mopidy-tidal/branch/master/graph/badge.svg?token=cTJDQ646wy)](https://codecov.io/gh/tehkillerbee/mopidy-tidal)

Mopidy Extension for Tidal music service integration.

### Changelog

Find the latest changelog [here](CHANGELOG.md)

### Contributions

- Current maintainer: [tehkillerbee](https://github.com/tehkillerbee)
- Original author: [mones88](https://github.com/mones88)
- [Contributors](https://github.com/tehkillerbee/mopidy-tidal/graphs/contributors)

Questions related to Mopidy-Tidal, feature suggestions, bug reports and Pull Requests are very welcome.

If you are experiencing playback issues unrelated to this plugin, please report this to the Mopidy-Tidal issue tracker
and/or check [Python-Tidal/Tidalapi repository](https://github.com/tamland/python-tidal) for relevant issues.

### Development guidelines

Please refer to [this document](DEVELOPMENT.md) to get you started.

### Getting started

First install and configure Mopidy as per the instructions
listed [here](https://docs.mopidy.com/en/latest/installation/). It is encouraged to install Mopidy as a systemd service,
as per the instructions listed [here](https://docs.mopidy.com/en/latest/running/service/).

After installing Mopidy, you can now proceed installing the plugins that you require, including Mopidy-Tidal. :

```
sudo pip3 install Mopidy-Tidal
```

##### Note: Make sure to install the Mopidy-Tidal plugin in the same python venv used by your Mopidy installation. Otherwise, the plugin will NOT be detected.

### Install from latest sources

In case you are upgrading your Mopidy-Tidal installation from the latest git sources, make sure to do a force upgrade
from the source root (remove both mopidy-tidal and python-tidal), followed by a (service) restart.

```
cd <mopidy-tidal source root>
sudo pip3 uninstall mopidy-tidal
sudo pip3 uninstall tidalapi
sudo pip3 install .
sudo systemctl restart mopidy
```

## Dependencies

### Python

Released versions of Mopidy-Tidal have the same requirement as the Mopidy
version they depend on. Development code may depend on unreleased features.
At the time of writing we require python >= 3.9 in anticipation of mopidy 3.5.0.

### Python-Tidal

Mopidy-Tidal requires the Python-Tidal API (tidalapi) to function. This is usually installed automatically when
installing Mopidy-Tidal.
In some cases, Python-Tidal stops working due to Tidal changing their API keys.

When this happens, it will usually be necessary to upgrade the Python-Tidal API plugin manually

```
sudo pip3 install --upgrade tidalapi
```

After upgrading Python-Tidal/tidalapi, it will often be necessary to delete the existing json file and restart mopidy.
The file is usually stored in `/var/lib/mopidy/tidal/tidal-<session_type>.json`, depending on your system configuration.

### GStreamer

When using High and Low quality, be sure to install gstreamer bad-plugins, e.g.:

```
sudo apt-get install gstreamer1.0-plugins-bad
```

This is mandatory to be able to play m4a streams and for playback of MPEG-DASH streams. Otherwise, you will likely get
an error:

```
WARNING  [MainThread] mopidy.audio.actor Could not find a application/x-hls decoder to handle media.
WARNING  [MainThread] mopidy.audio.gst GStreamer warning: No decoder available for type 'application/x-hls'.
ERROR    [MainThread] mopidy.audio.gst GStreamer error: Your GStreamer installation is missing a plug-in.
```

## Configuration

Before starting Mopidy, you must add configuration for Mopidy-Tidal to your Mopidy configuration file, if it is not
already present.
Run `sudo mopidyctl config` to see the current effective config used by Mopidy

The configuration is usually stored in `/etc/mopidy/mopidy.conf`, depending on your system configuration. Add the
configuration listed below in the respective configuration file and set the relevant fields.

Restart the Mopidy service after adding/changing the Tidal configuration
`sudo systemctl restart mopidy`

### Plugin configuration

The configuration is usually stored in `/etc/mopidy/mopidy.conf`, depending on your system configuration. Add the
configuration listed below in the respective configuration file and set the relevant fields.

```
[tidal]
enabled = true
quality = LOSSLESS
#playlist_cache_refresh_secs = 0
#lazy = true
#login_method = AUTO
#auth_method = OAUTH
#login_server_port = 8989
#client_id =
#client_secret =
```

### Plugin parameters

* **quality:** Set to one of the following quality types: `HI_RES_LOSSLESS`, `LOSSLESS`, `HIGH` or `LOW`. All quality
  levels are available with the standard (paid) subscription.
    * `HI_RES_LOSSLESS` provides HiRes lossless FLAC if available for the selected media.
    * `LOSSLESS` provides HiFi lossless FLAC if available.
    * `HIGH`, `LOW` provides M4A in either 320kbps or 96kbps bitrates.
* **auth_method (Optional):**: Select the authentication mode to use.
    * `OAUTH` used as default and currently allows playback in all available qualities, including `HI_RES_LOSSLESS`.
    * `PKCE` is optional, and allows `HI_RES_LOSSLESS`, `LOSSLESS` playback. This method uses the HTTP server for
      completing the second authentication step.
* **login_web_port (Optional):**: Port to use for the authentication HTTP Server. Default port: `8989`, i.e. web server
  will be available on `<host_ip:>:8989` eg. `localhost:8989`.
* **playlist_cache_refresh_secs (Optional):** Tells if (and how often) playlist
  content should be refreshed upon lookup.
    * `0` (default): The default value (`0`) means that playlists won't be refreshed after the
      extension has started, unless they are explicitly modified from mopidy.
    * `>0`: A non-zero value expresses for how long (in seconds) a cached playlist is
      considered valid. For example, a value of `300` means that the cached snapshot
      of a playlist will be used if a new `lookup` occurs within 5 minutes from the
      previous one, but the playlist will be re-loaded via API if a lookup request
      occurs later.

  The preferred setting for this value is a trade-off between UI responsiveness
  and responsiveness to changes. If you perform a lot of playlist changes from
  other clients and you want your playlists to be instantly updated on mopidy,
  then you may choose a low value for this setting, albeit this will result in
  longer waits when you look up a playlist, since it will be fetched from
  upstream most of the times. If instead you don't perform many playlist
  modifications, then you may choose a value for this setting within the range of
  hours - or days, or even leave it to zero so playlists will only be refreshed
  when mopidy restarts. This means that it will take longer for external changes
  to be reflected in the loaded playlists, but the UI will be more responsive
  when playlists are looked up. A value of zero makes the behaviour of
  `mopidy-tidal` quite akin to the current behaviour of `mopidy-spotify`.
* **lazy (Optional):**: Whether to connect lazily, i.e. when required, rather than
  at startup.
    * `false` (default): Lazy mode is off by default for backwards compatibility and to make the first login easier (
      since mopidy will not block in lazy mode until you try to access Tidal).
    * `true`: Mopidy-Tidal will only try to connect when something
      tries to access a resource provided by Tidal.

  Since failed connections due to
  network errors do not overwrite cached credentials (see below) and Mopidy
  handles exceptions in plugins gracefully, lazy mode allows Mopidy to continue to
  run even with intermittent or non-existent network access (although you will
  obviously be unable to play any streamed music if you cannot access the
  network). When the network comes back Mopidy will be able to play tidal content
  again. This may be desirable on mobile internet connections, or when a server
  is used with multiple backends and a failure with Tidal should not prevent
  other services from running.
* **login_method (Optional):**: This setting configures the auth login process.
    * `BLOCK` (default): The user is REQUIRED to complete the OAuth login flow, otherwise mopidy will hang.
    * `AUTO`/`HACK`: Mopidy will start as usual but the user will be prompted to complete the auth login flow by
      visiting a link. The link is provided as a dummy track and as a log message.
* **client_id, _secret (Optional):**: Tidal API `client_id`, `client_secret` can be overridden by the user if necessary.

## Login

Before TIDAL can be accessed from Mopidy, it is necessary to login, using either the OAuth or PKCE flow described below.

Both OAuth and PKCE flow require visiting an URL to complete the login process. The URL can be found either:

* In the Mopidy logs, as listed below

```
journalctl -u mopidy | tail -10
...
Visit link.tidal.com/AAAAA to log in, the code will expire in 300 seconds.
```

* Displayed in the Mopidy web client as a "dummy" track when the `login_method` is set to `AUTO`
* By playing the "dummy" track, a QR code will be displayed and the URL will be read aloud.
* Displayed as a link when accessing the auth. webserver `localhost:<login_web_port>` when PKCE authentication is used.

### General login tips

* When the `login_method` is set to BLOCK, all login processes are **blocking** actions, so Mopidy + Web interface will
  stop loading until you approve the application.
* When using the `lazy` mode, the login process will not be started until browsing the TIDAL related directories.
* Session is reloaded automatically when Mopidy is restarted. It will be necessary to perform these steps again if the
  json file is moved/deleted.

### OAuth Flow

When using OAuth authentication mode, you will be prompted to visit an URL to login.
This URL will be displayed in the Mopidy logs and/or in the Mopidy-Web client as a dummy track.

When prompted, visit the URL to complete the OAuth login flow. No extra steps are required.

### PKCE Flow

For `HI_RES` and `HI_RES_LOSSLESS` playback, the PKCE authentication method is required.
This PKCE flow also requires visiting an URL but requires an extra step to return the Tidal response URL to Python-Tidal

1. Visit the URL listed in the logs or in the Mopidy client. Usually, this should be `<host_ip>:<login_web_port>`, eg.
   localhost:8989. When running a headless server, make sure to use the correct IP.
2. You will be greeted with a link to the TIDAL login page and a form where you can paste the response URL:
   ![web_auth](docs/docs0.png)
3. Click the link and visit the TIDAL URL and login using your normal credentials.
4. Copy the complete URL of the page you were redirected to. This webpage normally lists "Oops" or something similar;
   this is normal.
5. Paste this URL into the web authentication page and click "Submit". You can now close the web page.
6. Refresh your Mopidy frontend. You should now be able to browse as usual.
