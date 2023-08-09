import pytest

from outpack.hash import Hash, hash_file, hash_parse, hash_string, hash_validate


def test_hash_string():
    assert hash_string("hello outpack", "md5") == Hash(
        "md5", "7c4d97e580abb6c2ffb8b1872907d84b"
    )


def test_parse_hash():
    h = "md5:7c4d97e580abb6c2ffb8b1872907d84b"
    assert hash_parse(h) == Hash("md5", "7c4d97e580abb6c2ffb8b1872907d84b")
    assert str(hash_parse(h)) == h


def test_hash_file(tmp_path):
    p = tmp_path / "file"
    with open(p, "wb") as f:
        f.write(bytes(range(256)))
    h = hash_file(p, "md5")
    assert h == Hash(algorithm="md5", digest="e2c865db4162bed963bfaa9ef6ac18f0")


def test_hash_validate_is_silent_on_success(tmp_path):
    p = tmp_path / "file"
    with open(p, "wb") as f:
        f.write(bytes(range(256)))
    h = Hash(algorithm="md5", digest="e2c865db4162bed963bfaa9ef6ac18f0")
    # Just a test that there is no error:
    hash_validate(p, h)

    h.algorithm = "sha1"
    with pytest.raises(Exception) as e:
        hash_validate(p, h)
    assert e.match("Hash of '.+' does not match:")
