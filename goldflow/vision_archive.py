"""Binance public data archive (data.binance.vision) client.

- Lists available files through the bucket's S3 listing API (paginated, never assumed).
- Streams downloads to `<file>.part` with HTTP Range resume, then atomically renames.
- Verifies every zip against Binance's published SHA-256 `.CHECKSUM` file.
"""
from __future__ import annotations
import hashlib, logging, os, re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from xml.etree import ElementTree as ET
import requests

from .retry import NonRetryable, backoff_sleep
from .fsutil import safe_remove

log = logging.getLogger("goldflow.archive")
NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

# dataset name -> (path segment, filename stem builder)
def archive_key(market: str, granularity: str, dataset: str, symbol: str, period: str) -> str:
    seg = {"futures_um": "futures/um", "futures_cm": "futures/cm", "spot": "spot"}[market]
    if dataset.startswith("klines_"):
        iv = dataset.split("_", 1)[1]
        return f"data/{seg}/{granularity}/klines/{symbol}/{iv}/{symbol}-{iv}-{period}.zip"
    return f"data/{seg}/{granularity}/{dataset}/{symbol}/{symbol}-{dataset}-{period}.zip"

@dataclass
class RemoteFile:
    key: str
    size: int
    etag: str
    last_modified: str

class VisionArchive:
    def __init__(self, base_url: str, list_url: str, timeout_s: int = 60, max_attempts: int = 8,
                 backoff_base_s: float = 1.0, backoff_cap_s: float = 60.0, chunk_bytes: int = 1 << 20):
        self.base_url, self.list_url = base_url.rstrip("/"), list_url.rstrip("/")
        self.timeout, self.max_attempts = timeout_s, max_attempts
        self.bb, self.bc, self.chunk = backoff_base_s, backoff_cap_s, chunk_bytes
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "goldflow-acquisition/0.2"

    # ------------------------------------------------------------------ listing
    def list_prefix(self, prefix: str) -> dict[str, RemoteFile]:
        """All objects under prefix (handles S3 pagination via marker)."""
        out: dict[str, RemoteFile] = {}
        marker = ""
        while True:
            params = {"prefix": prefix}
            if marker: params["marker"] = marker
            r = self._get_with_retry(self.list_url, params=params)
            root = ET.fromstring(r.content)
            contents = root.findall("s3:Contents", NS)
            for c in contents:
                k = c.find("s3:Key", NS).text
                out[k] = RemoteFile(k, int(c.find("s3:Size", NS).text), c.find("s3:ETag", NS).text.strip('"'),
                                    c.find("s3:LastModified", NS).text)
            if root.find("s3:IsTruncated", NS).text != "true" or not contents:
                break
            marker = contents[-1].find("s3:Key", NS).text
        return out

    # ------------------------------------------------------------------ http
    def _get_with_retry(self, url: str, **kw) -> requests.Response:
        last = None
        for attempt in range(self.max_attempts):
            try:
                r = self.s.get(url, timeout=self.timeout, **kw)
                if r.status_code == 404:
                    raise NonRetryable(f"404 Not Found: {url}", 404)
                if r.status_code in (429, 418) or r.status_code >= 500:
                    ra = float(r.headers.get("Retry-After", 0) or 0)
                    last = f"HTTP {r.status_code}"
                    backoff_sleep(attempt, self.bb, self.bc, floor=ra); continue
                r.raise_for_status()
                return r
            except NonRetryable:
                raise
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
                last = repr(e); backoff_sleep(attempt, self.bb, self.bc)
        raise RuntimeError(f"GET failed after {self.max_attempts} attempts: {url} ({last})")

    def fetch_checksum(self, key: str) -> str:
        txt = self._get_with_retry(f"{self.base_url}/{key}.CHECKSUM").text.strip()
        m = re.match(r"^([0-9a-fA-F]{64})\s+\*?(\S+)$", txt)
        if not m or m.group(2) != key.rsplit("/", 1)[-1]:
            raise RuntimeError(f"Malformed CHECKSUM for {key}: {txt!r}")
        return m.group(1).lower()

    def download(self, key: str, dest: Path, expected_size: int | None,
                 on_bytes: Callable[[int], None] | None = None) -> tuple[int, str, int]:
        """Download with resume. Returns (bytes, sha256, http_status). Raises NonRetryable on 404."""
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        url = f"{self.base_url}/{key}"
        status = 0
        for attempt in range(self.max_attempts):
            have = part.stat().st_size if part.exists() else 0
            if expected_size is not None and have > expected_size:
                safe_remove(part); have = 0
            headers = {"Range": f"bytes={have}-"} if have else {}
            try:
                with self.s.get(url, stream=True, timeout=self.timeout, headers=headers) as r:
                    status = r.status_code
                    if status == 404: raise NonRetryable(f"404 Not Found: {url}", 404)
                    if status == 416 and expected_size is not None and have == expected_size:
                        break  # already complete
                    if status in (429, 418) or status >= 500:
                        backoff_sleep(attempt, self.bb, self.bc, floor=float(r.headers.get("Retry-After", 0) or 0)); continue
                    r.raise_for_status()
                    mode = "ab" if (have and status == 206) else "wb"
                    with open(part, mode) as f:
                        for chunk in r.iter_content(self.chunk):
                            f.write(chunk)
                            if on_bytes: on_bytes(len(chunk))
                        f.flush(); os.fsync(f.fileno())
                size = part.stat().st_size
                if expected_size is not None and size != expected_size:
                    log.warning("size mismatch %s: %s != %s (attempt %d)", key, size, expected_size, attempt + 1)
                    backoff_sleep(attempt, self.bb, self.bc); continue
                break
            except NonRetryable:
                raise
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError, requests.exceptions.ChunkedEncodingError) as e:
                log.warning("download error %s attempt %d: %r", key, attempt + 1, e)
                backoff_sleep(attempt, self.bb, self.bc)
        else:
            raise RuntimeError(f"download failed after {self.max_attempts} attempts: {key}")
        sha = sha256_file(part)
        os.replace(part, dest)
        return dest.stat().st_size, sha, status

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""): h.update(b)
    return h.hexdigest()
