import base64
import json
from typing import Dict

from beartype import beartype

BASE64_PREFIX = "<THE_FOLLOWING_IS_BASE64_ENCODED>"


def hydrasafe_b64encode(s: str) -> str:
    s = base64.urlsafe_b64encode(s.encode("utf-8"))
    s = s.replace(b"=", b"/")  # TODO: am I really sure
    s = s.decode("utf-8")
    return f"{BASE64_PREFIX}{s}"


def hydrasafe_b64decode(s: str) -> str:
    s = base64.urlsafe_b64decode(s.replace("/", "="))
    s = s.decode("utf-8")
    return s


@beartype
def isBase64(s: str) -> bool:
    """
    see: https://stackoverflow.com/questions/12315398/check-if-a-string-is-encoded-in-base64-using-python
    return base64.b64encode(base64.b64decode(s)).decode("utf-8") == s
    WTF! how can this be "safe"?
    """
    return s.startswith(BASE64_PREFIX)


def maybe_decode_base64(s):
    """
    see: https://stackoverflow.com/questions/12315398/check-if-a-string-is-encoded-in-base64-using-python
    """
    if isinstance(s, str) and isBase64(s):
        o = hydrasafe_b64decode(s.replace(BASE64_PREFIX, ""))
        out = json.loads(o)
    else:
        out = s
    return out


def decode_base64_object_hook(dct: Dict):
    return {k: maybe_decode_base64(v) for k, v in dct.items()}


class Base64Decoder(json.JSONDecoder):
    """
    # see https://stackoverflow.com/questions/48991911/how-to-write-a-custom-json-decoder-for-a-complex-object
    """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=decode_base64_object_hook, *args, **kwargs
        )
