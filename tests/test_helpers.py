"""helpers/: checksum, serDe, file_upload."""
import hashlib
import os

import pytest

from helpers.checksum import compute, verify
from helpers.file_upload import read_document, read_file, save_document, save_file
from helpers.serDe import HEADER_SIZE, MAGIC, deserialize, serialize


# ---------------------------------------------------------------- checksum
class TestChecksum:
    def test_is_sha256_hex(self):
        c = compute(b"hello")
        assert c == hashlib.sha256(b"hello").hexdigest()
        assert len(c) == 64 and c == c.lower()

    def test_empty_data_has_a_checksum(self):
        assert compute(b"") == hashlib.sha256(b"").hexdigest()

    def test_verify_accepts_matching_data(self):
        assert verify(b"data", compute(b"data"))

    def test_verify_ignores_case_and_surrounding_spaces(self):
        assert verify(b"data", "  " + compute(b"data").upper() + "\n")

    @pytest.mark.parametrize("position", [0, 1, 99, 4999])
    def test_any_single_changed_byte_is_detected(self, position):
        data = bytearray(os.urandom(5000))
        expected = compute(bytes(data))
        data[position] ^= 0x01
        assert not verify(bytes(data), expected)

    def test_truncation_and_extension_are_detected(self):
        data = os.urandom(1000)
        expected = compute(data)
        assert not verify(data[:-1], expected)
        assert not verify(data + b"\x00", expected)

    @pytest.mark.parametrize("bad", [None, 123, b"abc", "", "not-a-checksum", "0" * 64])
    def test_verify_rejects_garbage_expected_values(self, bad):
        assert not verify(b"data", bad)


# -------------------------------------------------------------------- serDe
class TestSerDe:
    @pytest.mark.parametrize("size", [0, 1, 255, 4096, 1024 * 1024 + 7])
    def test_round_trip(self, size):
        data = os.urandom(size)
        assert deserialize(serialize(data)) == data

    def test_layout(self):
        blob = serialize(b"abc")
        assert blob[:4] == MAGIC == b"OBJ1"
        assert int.from_bytes(blob[4:12], "big") == 3
        assert blob[12:44] == hashlib.sha256(b"abc").digest()
        assert blob[HEADER_SIZE:] == b"abc"
        assert HEADER_SIZE == 44

    def test_content_is_never_decoded(self):
        data = bytes(range(256)) + b"\r\n\x00\xff\xfe"
        assert deserialize(serialize(data)) == data

    def test_rejects_data_without_the_header(self):
        for blob in (b"", b"OBJ1", b"x" * 100, b"OBJ2" + b"\x00" * 60):
            with pytest.raises(ValueError, match="bad header"):
                deserialize(blob)

    def test_rejects_truncated_payload(self):
        blob = serialize(b"x" * 100)
        with pytest.raises(ValueError, match="Incomplete"):
            deserialize(blob[:-1])

    def test_rejects_extra_bytes(self):
        with pytest.raises(ValueError, match="Incomplete"):
            deserialize(serialize(b"x" * 10) + b"extra")

    def test_rejects_a_changed_payload_byte(self):
        blob = bytearray(serialize(b"x" * 100))
        blob[HEADER_SIZE + 50] ^= 0xFF
        with pytest.raises(ValueError, match="corrupted"):
            deserialize(bytes(blob))

    def test_rejects_a_changed_checksum_in_the_header(self):
        blob = bytearray(serialize(b"x" * 100))
        blob[20] ^= 0xFF
        with pytest.raises(ValueError, match="corrupted"):
            deserialize(bytes(blob))


# -------------------------------------------------------------- file_upload
class TestFileUpload:
    def test_round_trip_is_byte_exact(self, tmp_path):
        data = bytes(range(256)) * 10 + b"\r\n\r\n"      # includes CRLF: must not be translated
        path = str(tmp_path / "f.bin")
        save_file(path, data)
        assert read_file(path) == data

    def test_aliases_the_client_uses(self):
        assert read_document is read_file and save_document is save_file

    def test_reading_a_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            read_file(str(tmp_path / "nope"))

    def test_empty_file(self, tmp_path):
        path = str(tmp_path / "empty")
        save_file(path, b"")
        assert read_file(path) == b""
