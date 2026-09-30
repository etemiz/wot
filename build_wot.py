from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import statistics
import sys

from wot.config import load_config
from wot.crawler import Crawler
from wot.store import TruStore, read_tru

log = logging.getLogger("wot")


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_dump(path: str, top_n: int = 20):
    scores = read_tru(path)
    vals = sorted(scores.values(), reverse=True)
    positive = [v for v in vals if v > 0.0]
    print(f"entries: {len(scores)}")
    if positive:
        print(f"positive: {len(positive)} min={min(positive):.6f} max={max(positive):.6f} mean={statistics.fmean(positive):.6f}")
    print("top:")
    ranked = sorted(scores.items(), key=lambda kv: -kv[1])
    for pk, v in ranked[:top_n]:
        print(f"  {pk.hex()} {v:.6f}")
    return 0


async def run(cfg, once: bool, stop_event: asyncio.Event):
    map_size = int(cfg.storage.map_size_gb * (1 << 30))
    store = TruStore(cfg.storage.lmdb_path, map_size)
    crawler = Crawler(cfg, store, stop_event)
    try:
        await crawler.run(once)
    finally:
        store.close()


async def amain(cfg, once: bool):
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await run(cfg, once, stop_event)


def main():
    parser = argparse.ArgumentParser(description="build a nostr web of trust in lmdb")
    parser.add_argument("--config", default=None, help="path to TOML config (default: wot.toml or wot.toml.example)")
    parser.add_argument("--once", action="store_true", help="batch crawl, score, write, exit (cron-friendly)")
    parser.add_argument("--dump", metavar="PATH", help="print stats and top scores from an lmdb store and exit")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)

    if args.dump:
        path = os.path.expanduser(args.dump)
        return cmd_dump(path)

    config_path = args.config
    if config_path is None:
        config_path = "wot.toml" if os.path.exists("wot.toml") else "wot.toml.example"
    if not os.path.exists(config_path):
        print(f"config not found: {config_path} (copy wot.toml.example to wot.toml and edit)", file=sys.stderr)
        return 1

    cfg = load_config(config_path)
    log.info(
        "config: %d roots, %d seed relays, %d history days, lmdb=%s",
        len(cfg.roots),
        len(cfg.relays.seed),
        cfg.crawl.history_days,
        cfg.storage.lmdb_path,
    )
    try:
        asyncio.run(amain(cfg, args.once))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())