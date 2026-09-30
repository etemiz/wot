from __future__ import annotations

import asyncio
import logging
import time

from wot import trust
from wot.config import Config, normalize_relay
from wot.graph import FollowGraph
from wot.relay_client import RelayConnection
from wot.store import TruStore

log = logging.getLogger("wot.crawler")

_DISCOVER_CHUNK = 250
_DISCOVER_TIMEOUT_S = 45.0
_DISCOVER_LIMIT = 1000


class Crawler:
    def __init__(self, cfg: Config, store: TruStore, stop_event: asyncio.Event):
        self.cfg = cfg
        self.store = store
        self.stop_event = stop_event
        self.graph = FollowGraph()
        self.root_ids = {self.graph.intern(pk): w for pk, w in cfg.roots.items()}
        self.conns: dict[str, RelayConnection] = {}
        self.seen_ids: set[bytes] = set()
        self.t: list[float] = []
        self.batch_since = 0
        self.live_mode = False
        self.prop_seeds: set[int] = set()
        self.dirty_w: set[int] = set()
        self.n_events = 0
        self.n_dedup = 0

    def add_relay(self, url: str) -> bool:
        if url in self.conns or url in self.cfg.relays.blacklist:
            return False
        if len(self.conns) >= self.cfg.relays.max_relays:
            return False
        conn = RelayConnection(
            url,
            url in self.cfg.relays.insecure,
            self._on_event,
            self.cfg.crawl.page_limit,
            self.cfg.crawl.eose_timeout_s,
            self.cfg.crawl.max_attempts,
        )
        conn.start()
        self.conns[url] = conn
        log.info("relay added: %s (%d total)", url, len(self.conns))
        return True

    def _on_event(self, url: str, ev: dict):
        if ev["kind"] != 3:
            return
        eid = bytes.fromhex(ev["id"])
        if eid in self.seen_ids:
            self.n_dedup += 1
            return
        self.seen_ids.add(eid)
        self.n_events += 1
        author = bytes.fromhex(ev["pubkey"])
        pks = extract_p_tags(ev["tags"])
        if self.live_mode:
            if self.graph.is_newer(author, ev["created_at"], eid):
                dirty = self.graph.replace_follows(author, pks)
                self.dirty_w |= dirty
                n = len(self.graph.pubkey_of)
                if len(self.t) < n:
                    self.t.extend([0.0] * (n - len(self.t)))
                aid = self.graph.id_of[author]
                if self.t[aid] > 0.0:
                    self.prop_seeds.add(aid)
        else:
            self.graph.apply_contact_event(author, ev["created_at"], eid, pks)

    async def _oneshot_relay(self, url: str, authors_hex: list[str]) -> list[dict]:
        conn = self.conns.get(url)
        if conn is None:
            return []
        filters = {"kinds": [10002], "authors": authors_hex, "limit": _DISCOVER_LIMIT}
        rq: asyncio.Queue = asyncio.Queue()
        conn.send_command(("oneshot", filters, rq, _DISCOVER_TIMEOUT_S))
        try:
            return await asyncio.wait_for(rq.get(), _DISCOVER_TIMEOUT_S + 30)
        except asyncio.TimeoutError:
            return []

    async def _discover(self, author_ids: list[int]) -> list[str]:
        if not author_ids:
            return []
        newest: dict[str, tuple[int, list]] = {}
        seen: set[bytes] = set()
        hexes = [self.graph.pubkey_of[i].hex() for i in author_ids]
        chunks = [hexes[i : i + _DISCOVER_CHUNK] for i in range(0, len(hexes), _DISCOVER_CHUNK)]
        for chunk in chunks:
            if self.stop_event.is_set():
                break
            results = await asyncio.gather(
                *(self._oneshot_relay(url, chunk) for url in list(self.conns)),
                return_exceptions=True,
            )
            for evs in results:
                if not isinstance(evs, list):
                    continue
                for ev in evs:
                    eid = ev.get("id", "")
                    if eid in seen:
                        continue
                    seen.add(eid)
                    author = ev["pubkey"]
                    ts = ev["created_at"]
                    cur = newest.get(author)
                    if cur is None or ts > cur[0]:
                        newest[author] = (ts, ev["tags"])
        counts: dict[str, int] = {}
        for _author, (_ts, tags) in newest.items():
            relays = set()
            for tag in tags:
                if isinstance(tag, list) and len(tag) >= 2 and tag[0] == "r":
                    url = normalize_relay(tag[1]) if isinstance(tag[1], str) else None
                    write = len(tag) < 3 or tag[2] == "write"
                    if url and write:
                        relays.add(url)
            for url in relays:
                counts[url] = counts.get(url, 0) + 1
        ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        added = []
        for url, count in ranked:
            if len(self.conns) >= self.cfg.relays.max_relays:
                break
            if url not in self.conns and url not in self.cfg.relays.blacklist:
                if self.add_relay(url):
                    added.append(url)
        if added:
            log.info("discovered %d new relays (from %d authors)", len(added), len(author_ids))
        return added

    async def _batch_crawl(self, urls: list[str]):
        if not urls:
            return
        if self.batch_since == 0:
            self.batch_since = int(time.time()) - self.cfg.crawl.history_days * 86400
        floor = self.batch_since
        futures = {}
        for url in urls:
            rq: asyncio.Queue = asyncio.Queue()
            self.conns[url].send_command(("batch", self.batch_since, floor, rq))
            futures[url] = rq
        deadline = time.monotonic() + self.cfg.crawl.batch_timeout_s
        pending = {asyncio.create_task(self._wait_reply(url, rq)) for url, rq in futures.items()}
        n_done = n_failed = 0
        while pending:
            if self.stop_event.is_set():
                break
            remaining = min(deadline - time.monotonic(), 5.0)
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(pending, timeout=remaining)
            for fut in done:
                url, status = fut.result()
                if isinstance(status, tuple):
                    status, count = status
                else:
                    count = 0
                if status == "done":
                    n_done += 1
                    log.info("%s: %d events", url, count)
                else:
                    n_failed += 1
                    log.warning("%s: batch crawl failed (%d events)", url, count)
        for fut in pending:
            fut.cancel()
        if pending:
            log.warning(
                "batch crawl ended with %d relays unfinished (stopped=%s)",
                len(pending),
                self.stop_event.is_set(),
            )
        log.info(
            "batch crawl finished: %d done, %d failed, %d timed out; events=%d dedup=%d",
            n_done,
            n_failed,
            len(pending),
            self.n_events,
            self.n_dedup,
        )

    async def _wait_reply(self, url: str, rq: asyncio.Queue):
        status = await rq.get()
        return url, status

    def _build_and_score(self) -> int:
        t0 = time.monotonic()
        cfg = self.cfg.trust
        edges = self.graph.build_edges()
        self.t = trust.propagate(self.graph, self.root_ids, cfg.decay)
        w = trust.follower_scores(self.graph, self.t, cfg.decay, cfg.top_n, cfg.hub_min_follows, cfg.hub_exponent)
        pairs = trust.score(self.graph, self.t, w)
        n = self.store.write_all(pairs)
        elapsed = time.monotonic() - t0
        log.info(
            "scored %d accounts (%d authors, %d edges) in %.1fs -> %s",
            n,
            len(self.graph.follows),
            edges,
            elapsed,
            self.store.env.path(),
        )
        return n

    def _top_trusted_ids(self) -> list[int]:
        if not self.t:
            return []
        pk = self.graph.pubkey_of
        ids = [i for i, v in enumerate(self.t) if v > 0.0]
        ids.sort(key=lambda i: (-self.t[i], pk[i]))
        return ids[: self.cfg.relays.discover_top]

    def _process_prop(self):
        if not self.prop_seeds and len(self.t) >= len(self.graph.pubkey_of):
            return
        n = len(self.graph.pubkey_of)
        if len(self.t) < n:
            self.t.extend([0.0] * (n - len(self.t)))
        seeds = self.prop_seeds
        self.prop_seeds = set()
        changed = trust.propagate_from(self.graph, self.t, seeds, self.cfg.trust.decay)
        follows = self.graph.follows
        for u in changed:
            self.dirty_w.add(u)
            self.dirty_w |= follows[u]

    def _rescore_dirty(self):
        if not self.dirty_w:
            return
        cfg = self.cfg.trust
        t = self.t
        pk_of = self.graph.pubkey_of
        changed: dict[bytes, float | None] = {}
        for u in self.dirty_w:
            w = trust.follower_score_of(self.graph, t, cfg.decay, cfg.top_n, cfg.hub_min_follows, cfg.hub_exponent, u)
            changed[pk_of[u]] = w
        self.dirty_w = set()
        if changed:
            self.store.update(changed)
            log.info("live recompute: %d scores updated", len(changed))

    async def _run_live(self):
        self.live_mode = True
        for conn in self.conns.values():
            conn.send_command(("live", self.batch_since))
        interval = self.cfg.live.recompute_interval_s
        refresh = self.cfg.live.batch_refresh_s
        log.info(
            "live mode started (recompute every %ds, rebatch every %ds)",
            int(interval),
            int(refresh) if refresh > 0 else -1,
        )
        next_tick = time.monotonic() + interval
        next_rebatch = time.monotonic() + refresh if refresh > 0 else float("inf")
        while not self.stop_event.is_set():
            now = time.monotonic()
            wake = min(next_tick, next_rebatch) - now
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=max(0.0, wake))
            except asyncio.TimeoutError:
                pass
            if self.stop_event.is_set():
                break
            now = time.monotonic()
            if now >= next_rebatch:
                next_rebatch = now + refresh
                try:
                    await self._rebatch()
                except Exception:
                    log.exception("scheduled rebatch failed")
                next_tick = time.monotonic() + interval
            elif now >= next_tick:
                next_tick = now + interval
                try:
                    self._process_prop()
                    self._rescore_dirty()
                except Exception:
                    log.exception("live recompute failed")

    async def _rebatch(self):
        log.info("scheduled rebatch starting")
        self.batch_since = int(time.time()) - self.cfg.crawl.history_days * 86400
        await self._batch_crawl(list(self.conns))
        if self.stop_event.is_set():
            return
        self._build_and_score()
        if self.cfg.relays.discover:
            added = await self._discover(self._top_trusted_ids())
            if added and not self.stop_event.is_set():
                await self._batch_crawl(added)
                self._build_and_score()
        for conn in self.conns.values():
            conn.send_command(("live", self.batch_since))
        log.info("scheduled rebatch complete")

    async def stop(self):
        await asyncio.gather(*(c.stop() for c in self.conns.values()), return_exceptions=True)

    async def run(self, once: bool):
        try:
            for url in self.cfg.relays.seed:
                self.add_relay(url)
            if self.cfg.relays.discover:
                await self._discover(list(self.root_ids))
            if self.stop_event.is_set():
                return
            await self._batch_crawl(list(self.conns))
            if self.stop_event.is_set():
                return
            self._build_and_score()
            if self.cfg.relays.discover:
                added = await self._discover(self._top_trusted_ids())
                if added and not self.stop_event.is_set():
                    await self._batch_crawl(added)
                    self._build_and_score()
            if not once and not self.stop_event.is_set():
                await self._run_live()
        finally:
            await self.stop()


def extract_p_tags(tags) -> list[bytes]:
    out = []
    append = out.append
    fromhex = bytes.fromhex
    for tag in tags:
        if (
            isinstance(tag, list)
            and len(tag) >= 2
            and tag[0] == "p"
            and isinstance(tag[1], str)
            and len(tag[1]) == 64
        ):
            try:
                append(fromhex(tag[1]))
            except ValueError:
                pass
    return out