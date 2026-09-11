"""Bounded, unauthenticated public acquisition with immutable raw provenance."""
from __future__ import annotations
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import requests

ALLOWED_HOSTS = {'clob.polymarket.com', 'gamma-api.polymarket.com',
                 'data-api.polymarket.com', 'aviationweather.gov', 'mesonet.agron.iastate.edu'}

class PublicCache:
    def __init__(self, root: Path, *, max_requests: int = 100, max_bytes: int = 20_000_000,
                 timeout: float = 15, offline: bool = False):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.max_requests, self.max_bytes, self.timeout = max_requests, max_bytes, timeout
        self.requests = 0
        self.offline = offline
        self.session = requests.Session()
        self.session.trust_env = False  # never consult netrc or authentication environment
        self.session.headers['User-Agent'] = 'paper-remediation-public-research/1.0'

    def get_text(self, url: str) -> str:
        parsed = urlparse(url)
        if parsed.scheme != 'https' or parsed.hostname not in ALLOWED_HOSTS or parsed.username or parsed.password:
            raise ValueError('public endpoint is not allowlisted')
        paths = {
            'aviationweather.gov': {'/api/data/metar', '/api/data/stationinfo'},
            'mesonet.agron.iastate.edu': {'/cgi-bin/request/asos.py'},
            'data-api.polymarket.com': {'/trades'},
            'gamma-api.polymarket.com': {'/markets', '/events'},
            'clob.polymarket.com': {'/book', '/prices-history'},
        }
        if parsed.path not in paths[parsed.hostname] and not (parsed.hostname == 'clob.polymarket.com' and parsed.path.startswith('/markets/')):
            raise ValueError('public endpoint path is not allowlisted')
        if {'user','address','proxyWallet','funder','api_key'} & parse_qs(parsed.query).keys():
            raise ValueError('account-scoped public requests are disabled')
        key = hashlib.sha256(url.encode()).hexdigest()
        raw, meta = self.root/f'{key}.raw', self.root/f'{key}.json'
        if raw.exists() and meta.exists():
            body = raw.read_bytes()
            record = json.loads(meta.read_text())
            if record.get('url') != url or hashlib.sha256(body).hexdigest() != record['sha256']:
                raise ValueError('cache checksum mismatch')
            return body.decode('utf-8')
        if self.offline:
            raise ValueError('offline cache miss')
        if self.requests >= self.max_requests:
            raise ValueError('public request budget exhausted')
        used = sum(p.stat().st_size for p in self.root.iterdir() if p.is_file())
        budget = min(2_000_000, self.max_bytes-used-4096)
        if budget <= 0:
            raise ValueError('cache disk budget exhausted')
        self.requests += 1
        with self.session.get(url, timeout=self.timeout, stream=True, allow_redirects=False) as response:
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError(f'public response status {response.status_code}')
            body = bytearray()
            for chunk in response.iter_content(65536):
                body.extend(chunk)
                if len(body) > budget:
                    raise ValueError('public response exceeds byte budget')
            text = bytes(body).decode('utf-8')
            record = {'url': url, 'acquired_at': datetime.now(timezone.utc).isoformat(),
                      'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body),
                      'status': response.status_code, 'point_in_time_forecast': False}
        # Never overwrite an earlier acquisition. Use a new cache directory for a new snapshot.
        with raw.open('xb') as f:
            f.write(body)
        with meta.open('x') as f:
            json.dump(record, f, indent=2)
        return text

    def get_json(self, url: str):
        return json.loads(self.get_text(url))


    def provenance(self, url: str) -> dict:
        key = hashlib.sha256(url.encode()).hexdigest()
        record = json.loads((self.root / f'{key}.json').read_text())
        if record['url'] != url:
            raise ValueError('provenance URL mismatch')
        return record
