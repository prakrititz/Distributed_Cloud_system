"""helpers/: checksum, file_upload."""
import hashlib
import os

import pytest

from helpers.checksum import compute, verify
from helpers.file_upload import read_document, read_file, save_document, save_file


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
