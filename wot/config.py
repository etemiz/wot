from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field

_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
_GLYPH = {c: i for i, c in enumerate(_CHARSET)}


def _polymod(values):
    gen = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
    chk = 1
    for v in values:
        b = chk >> 25
        chk = ((chk & 0x1FFFFFF) << 5) ^ v
        for i in range(5):
            if (b >> i) & 1:
                chk ^= gen[i]
    return chk


def _hrp_expand(hrp):
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]


def _convert_bits(data, from_bits, to_bits, pad):
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << to_bits) - 1
    for value in data:
        if value < 0 or (value >> from_bits):
            raise ValueError("invalid value")
        acc = (acc << from_bits) | value
        bits += from_bits
        while bits >= to_bits:
            bits -= to_bits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (to_bits - bits)) & maxv)
    elif bits >= from_bits or ((acc << (to_bits - bits)) & maxv):
        raise ValueError("invalid padding")
    return ret


def decode_npub(s: str) -> bytes:
    s = s.strip()
    if s != s.lower() and s != s.upper():
        raise ValueError("mixed case bech32 string")
    s = s.lower()
    pos = s.rfind("1")
    if pos < 1 or pos + 7 > len(s):
        raise ValueError("invalid bech32 separator position")
    hrp = s[:pos]
    data = s[pos + 1 :]
    if not all(c in _GLYPH for c in data):
        raise ValueError("invalid bech32 data character")
    if _polymod(_hrp_expand(hrp) + [_GLYPH[c] for c in data]) != 1:
        raise ValueError("invalid bech32 checksum")
    raw = bytes(_convert_bits([_GLYPH[c] for c in data[:-6]], 5, 8, False))
    if hrp != "npub":
        raise ValueError(f"expected npub hrp, got {hrp!r}")
    if len(raw) != 32:
        raise ValueError("npub must decode to 32 bytes")
    return raw


def decode_pubkey(s: str) -> bytes:
    s = s.strip().lower()
    if len(s) == 64 and all(c in "0123456789abcdef" for c in s):
        return bytes.fromhex(s)
    return decode_npub(s)


@dataclass
class RelaysConfig:
    seed: list[str] = field(default_factory=list)
    blacklist: list[str] = field(default_factory=list)
    insecure: list[str] = field(default_factory=list)
    max_relays: int = 50
    discover: bool = True
    discover_top: int = 1000


@dataclass
class CrawlConfig:
    history_days: int = 21
    page_limit: int = 500
    batch_timeout_s: float = 3600.0
    eose_timeout_s: float = 60.0
    max_attempts: int = 5


@dataclass
class TrustConfig:
    decay: float = 0.7
    top_n: int = 10
    hub_min_follows: int = 20
    hub_exponent: float = 0.3


@dataclass
class StorageConfig:
    lmdb_path: str = "wot-db"
    map_size_gb: float = 10.0


@dataclass
class LiveConfig:
    recompute_interval_s: float = 300.0
    batch_refresh_s: float = 0.0


@dataclass
class Config:
    relays: RelaysConfig
    roots: dict[bytes, float]
    crawl: CrawlConfig
    trust: TrustConfig
    storage: StorageConfig
    live: LiveConfig


def normalize_relay(url: str) -> str | None:
    url = url.strip().rstrip("/")
    if not url:
        return None
    if "://" not in url:
        url = "wss://" + url
    scheme, _, rest = url.partition("://")
    if scheme not in ("wss", "ws") or not rest:
        return None
    return f"{scheme}://{rest.lower()}"


def _merge(section: dict, defaults: dict, name: str) -> dict:
    out = dict(defaults)
    for k, v in section.items():
        if k not in defaults:
            raise ValueError(f"unknown key {name}.{k}")
        out[k] = v
    return out


def load_config(path: str) -> Config:
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    for key in ("relays", "roots"):
        if key not in raw:
            raise ValueError(f"missing [{key}] section")

    relays = RelaysConfig(**_merge(raw.get("relays", {}), RelaysConfig().__dict__, "relays"))
    crawl = CrawlConfig(**_merge(raw.get("crawl", {}), CrawlConfig().__dict__, "crawl"))
    trust = TrustConfig(**_merge(raw.get("trust", {}), TrustConfig().__dict__, "trust"))
    storage = StorageConfig(**_merge(raw.get("storage", {}), StorageConfig().__dict__, "storage"))
    live = LiveConfig(**_merge(raw.get("live", {}), LiveConfig().__dict__, "live"))

    roots = {}
    for k, w in raw["roots"].items():
        pk = decode_pubkey(k)
        if not isinstance(w, (int, float)) or not (0.0 < float(w) <= 1.0):
            raise ValueError(f"root weight for {k[:12]}... must be in (0, 1]")
        roots[pk] = float(w)
    if not roots:
        raise ValueError("no roots configured")

    relays.seed = [u for u in (normalize_relay(u) for u in relays.seed) if u]
    relays.blacklist = {u for u in (normalize_relay(u) for u in relays.blacklist) if u}
    relays.insecure = {u for u in (normalize_relay(u) for u in relays.insecure) if u}
    relays.seed = [u for u in relays.seed if u not in relays.blacklist]
    if not relays.seed:
        raise ValueError("no usable seed relays")

    if not (0.0 < trust.decay < 1.0):
        raise ValueError("trust.decay must be in (0, 1)")
    if trust.top_n < 1:
        raise ValueError("trust.top_n must be >= 1")
    if crawl.history_days < 1:
        raise ValueError("crawl.history_days must be >= 1")

    storage.lmdb_path = os.path.expanduser(storage.lmdb_path)
    return Config(relays, roots, crawl, trust, storage, live)