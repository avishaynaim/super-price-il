import gzip
import io
import zipfile

from src.scraper.base import _decompress


def test_plain_gzip_roundtrip():
    raw = gzip.compress(b"<root/>")
    assert _decompress(raw) == b"<root/>"


def test_zip_wrapping_binaprojects_case():
    # binaprojects ships .gz-named files that are actually ZIPs.
    inner = b"<root>hello</root>"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("doc.xml", inner)
    assert _decompress(buf.getvalue()) == inner


def test_zip_with_gzipped_inner_member():
    inner = gzip.compress(b"<root>nested</root>")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("doc.xml.gz", inner)
    assert _decompress(buf.getvalue()) == b"<root>nested</root>"


def test_uncompressed_passes_through():
    assert _decompress(b"<root/>") == b"<root/>"
