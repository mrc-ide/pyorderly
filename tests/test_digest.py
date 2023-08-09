import pytest

from outpack.digest import (
    Digest,
    digest_file,
    digest_parse,
    digest_string,
    digest_validate,
)


def test_digest_string():
    assert digest_string("hello outpack", "md5") == Digest(
        "md5", "7c4d97e580abb6c2ffb8b1872907d84b"
    )


def test_parse_digest():
    x = "md5:7c4d97e580abb6c2ffb8b1872907d84b"
    assert digest_parse(x) == Digest("md5", "7c4d97e580abb6c2ffb8b1872907d84b")
    assert str(digest_parse(x)) == x


def test_digest_file(tmp_path):
    p = tmp_path / "file"
    with open(p, "wb") as f:
        f.write(bytes(range(256)))
    x = digest_file(p, "md5")
    assert x == Digest(
        algorithm="md5", value="e2c865db4162bed963bfaa9ef6ac18f0"
    )


def test_digest_validate_is_silent_on_success(tmp_path):
    p = tmp_path / "file"
    with open(p, "wb") as f:
        f.write(bytes(range(256)))
    h = Digest(algorithm="md5", value="e2c865db4162bed963bfaa9ef6ac18f0")
    # Just a test that there is no error:
    digest_validate(p, h)

    h.algorithm = "sha1"
    with pytest.raises(Exception) as e:
        digest_validate(p, h)
    assert e.match("Digest of '.+' does not match:")
