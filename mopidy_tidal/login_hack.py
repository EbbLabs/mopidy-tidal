import inspect
from abc import ABC
from functools import reduce, wraps
from itertools import chain
from logging import getLogger
from types import FunctionType
from typing import TYPE_CHECKING, Optional, Union, get_args, get_origin
from urllib.parse import urlencode

from mopidy.models import Album, Artist, Image, Playlist, Ref, SearchResult, Track
from requests import get

if TYPE_CHECKING:  # pragma: no cover
    from backend import TidalBackend

__all__ = ["login_hack", "speak_login_hack"]

NoneType = type(None)

UNION_TYPES = {Union}

try:  # pragma: no cover
    from types import UnionType

    UNION_TYPES |= {UnionType}
except ImportError:  # pragma: no cover
    pass

logger = getLogger(__name__)


def extract_types(possibly_union_type) -> list:
    """Extract the real type from an optional type."""
    if get_origin(possibly_union_type) in UNION_TYPES:
        return nonnull_types(possibly_union_type)
    return [possibly_union_type]


def nonnull_types(t):
    return [x for x in get_args(t) if x is not NoneType]


def interesting_types(t) -> set:
    """Find all the interesting types in a type, as a flat list."""
    if base_type := get_origin(t):
        if base_type is list:
            return set(chain.from_iterable(interesting_types(x) for x in get_args(t)))
        if base_type is dict:
            return interesting_types(get_args(t)[1])
    return {t}


class Builder(ABC):
    mapping: dict

    def build(self, t):
        if supertype := get_origin(t):
            subtypes = nonnull_types(t)
            if supertype is list:
                return [self.build(subtypes[0])]
            elif supertype is set:
                return {self.build(subtypes[0])}
            else:
                assert supertype is dict
                k, v = subtypes
                return {self.build(k): self.build(v)}
        else:
            return self.mapping[t]()


class ObjectBuilder(Builder):
    width = height = 150

    def __init__(self, *_, schema: str, uri: str, url: str, msg: str, **kwargs):
        self.schema = schema
        self.uri = uri
        self.url = url
        self.msg = msg
        self.mapping = {
            str: self._login_uri,
            Playlist: lambda: Playlist(
                uri="tidal:playlist:login",
                name=self.msg,
                tracks=[self.build(Track)],
            ),
            Ref: lambda: Ref(
                name=self.msg,
                type=self.ref_type(),
                uri=self.uri,
            ),
            Ref.playlist: lambda: Ref.playlist(
                name=self.msg,
                uri="tidal:playlist:login",
            ),
            Track: lambda: Track(uri="tidal:track:login", name=self.msg),
            Artist: lambda: Artist(uri="tidal:artist:login", name=self.msg),
            Album: lambda: Album(uri="tidal:album:login", name=self.msg),
            Image: lambda: Image(
                uri=self._image_url(), width=self.width, height=self.height
            ),
            SearchResult: lambda: SearchResult(
                artists=[self.build(Artist)],
                albums=[self.build(Album)],
                tracks=[self.build(Track)],
            ),
        }

    def _image_url(self) -> str:
        """Link to a qr code encoding the login url."""
        return "https://api.qrserver.com/v1/create-qr-code/?" + urlencode(
            dict(size=f"{self.width}x{self.height}", data=self.url)
        )

    def ref_type(self):
        return "directory" if self.schema.endswith("s") else self.schema

    def _login_uri(self) -> str:
        return self.uri if self.uri else f"tidal:{self.schema}:login"


class PassthroughBuilder(Builder):
    def __init__(self, *_, by_type: dict):
        self.mapping = by_type


def doublewrap(fn):
    """Double decorate to allow version with/without args.

    See https://stackoverflow.com/a/14412901/15452601
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        without_args = (
            len(args) == 1 and not kwargs and isinstance(args[0], FunctionType)
        )
        if without_args:
            return fn(args[0])
        else:
            # pass args/kwargs through to decorated fn
            return lambda f: fn(f, *args, **kwargs)

    return wrapper


def find_uri(args_mapping, kwargs):
    uri = None
    for k in ("uri", "uris"):
        for mapping in (args_mapping, kwargs):
            if v := mapping.get(k):
                uri = v
    if isinstance(uri, list):
        uri = uri[0]

    return uri


@doublewrap
def login_hack(fn, type=None, passthrough=False):
    manual_return_type = type

    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        backend: "TidalBackend" = obj.backend
        if not backend.logged_in and backend.login_method == "HACK":
            schema = ""
            uri = ""
            expected_return_type = NoneType
            spec = inspect.getfullargspec(fn)
            # we only need the first vararg anyhow, but this solution is not general
            spec_args = spec.args + [spec.varargs]
            args_mapping = {
                k: next(iter(args[i : i + 1]), None)
                for i, k in enumerate(spec_args[1:])
            }
            if "uri" in repr(spec):
                uri = find_uri(args_mapping, kwargs)
                if uri:
                    _, schema, *_ = uri.split(":")
            elif "field" in spec.args:
                schema = kwargs.get("field", args_mapping["field"])

            if schema:
                type_mapping = {
                    "artists": Artist,
                    "albums": Album,
                    "playlists": Playlist,
                    "tracks": Track,
                    "moods": Ref,
                    "mixes": Ref,
                    "genres": Ref,
                }
                plural = (
                    schema
                    if schema.endswith("s")
                    else schema + ("es" if schema.endswith("x") else "s")
                ).replace("my_", "")
                expected_return_type = type_mapping.get(plural, NoneType)

            if manual_return_type:
                return_type = manual_return_type
            else:
                # Assume all declared return types have the same structure
                declared_return_types = extract_types(fn.__annotations__["return"])
                possible_return_types = reduce(
                    lambda a, b: a | interesting_types(b),
                    [set(), *declared_return_types],
                )
                expected_type_possible = (
                    expected_return_type is not NoneType
                    and interesting_types(expected_return_type).issubset(
                        possible_return_types
                    )
                )
                return_type = (
                    match_structure(declared_return_types[0], expected_return_type)
                    if expected_type_possible
                    else declared_return_types[0]
                )

            url = backend.login_url
            msg = f"Please visit {url} to log in."

            logger.info("Not logged in. " + msg)
            if passthrough:
                return PassthroughBuilder(by_type={str: lambda: msg}).build(return_type)
            else:
                return ObjectBuilder(schema=schema, uri=uri, url=url, msg=msg).build(
                    return_type
                )
        elif backend.login_method == "HACK":
            audio_helper = LoginAudioHelper(backend)
            audio_helper.remove()

        return fn(obj, *args, **kwargs)

    return wrapper


def match_structure(target_type, inner_type):
    """Return a type struture equivalent to the target but with the right inner type."""
    base_type = get_origin(target_type)
    if base_type is dict:
        return base_type[get_args(target_type)[0], inner_type]
    if base_type is list:
        return base_type[inner_type]
    else:
        return inner_type


voice_rss_api_key = "eb909fe9f2ce403bb7209de172d096f1"


def speech_url(msg: str) -> str:
    return "https://api.voicerss.org?" + urlencode(
        dict(
            key=voice_rss_api_key,
            hl="en-gb",
            c="OGG",
            f="16khz_16_bit_stereo",
            src=msg,
        )
    )


class LoginAudioHelper:
    def __init__(self, backend: "TidalBackend"):
        self.backend = backend
        self.outdir = backend.data_dir / "login_audio"
        url = self.backend.login_url
        self._audiof = None
        if url:
            *_, code = url.split("/")
            self._audiof = self.outdir / f"{code}.ogg"

    def remove(self):
        if self.outdir.exists():
            for x in self.outdir.glob("*.ogg"):
                x.unlink()

    def download(self, url: str) -> Optional[str]:
        self.outdir.mkdir(parents=True, exist_ok=True)
        r = get(url)
        try:
            r.raise_for_status()
        except Exception:
            return None
        assert self._audiof
        with self._audiof.open("wb") as f:
            f.write(r.content)
        return self._audiof.as_uri()


def speak_login_hack(fn):
    @wraps(fn)
    def wrapper(obj, *args, **kwargs):
        backend: "TidalBackend" = obj.backend
        if not backend.logged_in and backend.login_method == "HACK":
            audio_helper = LoginAudioHelper(backend)
            url = backend.login_url
            msg = f"Please visit {url}, log in, and add this device.   Then come back and refresh to remove this message."
            return audio_helper.download(speech_url(msg))
        else:
            return fn(obj, *args, **kwargs)

    return wrapper
