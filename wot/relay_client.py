from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import ssl
import time

from websockets.asyncio.client import connect

log = logging.getLogger("wot.relay")

_HEX64 = re.compile(r"\A[0-9a-f]{64}\Z")
_MAX_FRAME = 64 * 1024 * 1024


def valid_event(ev) -> bool:
    if not isinstance(ev, dict):
        return False
    pk = ev.get("pubkey")
    eid = ev.get("id")
    if not isinstance(pk, str) or not _HEX64.match(pk):
        return False
    if not isinstance(eid, str) or not _HEX64.match(eid):
        return False
    ca = ev.get("created_at")
    if not isinstance(ca, int) or isinstance(ca, bool):
        return False
    if ca < 0 or ca > time.time() + 600:
        return False
    if not isinstance(ev.get("kind"), int) or isinstance(ev.get("kind"), bool):
        return False
    return isinstance(ev.get("tags"), list)


class RelayConnection:
    def __init__(
        self,
        url: str,
        insecure: bool,
        on_event,
        page_limit: int,
        eose_timeout_s: float,
        max_attempts: int,
    ):
        self.url = url
        self.on_event = on_event
        self.page_limit = page_limit
        self.eose_timeout_s = eose_timeout_s
        self.max_attempts = max_attempts
        if insecure:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self.ssl_ctx = ctx
        else:
            self.ssl_ctx = ssl.create_default_context()
        self.secure = url.startswith("wss://")
        self.queue: asyncio.Queue = asyncio.Queue()
        self.task: asyncio.Task | None = None
        self.stopping = False

    def start(self):
        self.task = asyncio.create_task(self._run(), name=f"relay-{self.url}")

    async def stop(self):
        self.stopping = True
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None

    def send_command(self, cmd: tuple):
        self.queue.put_nowait(cmd)

    async def _run(self):
        live_task: asyncio.Task | None = None
        try:
            while True:
                cmd = await self.queue.get()
                if cmd is None or cmd[0] == "stop":
                    return
                try:
                    if cmd[0] == "live":
                        if live_task is not None:
                            await self._interrupt(live_task)
                        live_task = asyncio.create_task(self._do_live(cmd[1]))
                    elif cmd[0] == "oneshot":
                        await self._interrupt(live_task)
                        live_task = None
                        await self._do_oneshot(cmd[1], cmd[2], cmd[3])
                    elif cmd[0] == "batch":
                        await self._interrupt(live_task)
                        live_task = None
                        await self._do_batch(cmd[1], cmd[2], cmd[3])
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("%s: command %r failed: %r", self.url, cmd[0], e)
                    reply_q = None
                    if cmd[0] == "batch" and len(cmd) >= 4:
                        reply_q = cmd[3]
                    elif cmd[0] == "oneshot" and len(cmd) >= 3:
                        reply_q = cmd[2]
                    if isinstance(reply_q, asyncio.Queue):
                        if cmd[0] == "batch":
                            reply_q.put_nowait(("failed", 0))
                        elif cmd[0] == "oneshot":
                            reply_q.put_nowait([])
        finally:
            if live_task is not None:
                await self._interrupt(live_task)

    @staticmethod
    async def _interrupt(task: asyncio.Task):
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def _connect(self):
        return connect(
            self.url,
            ssl=self.ssl_ctx if self.secure else None,
            max_size=_MAX_FRAME,
            open_timeout=30,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
        )

    async def _do_oneshot(self, filters: dict, reply_q: asyncio.Queue, timeout_s: float):
        sub = "wot-oneshot"
        events = []
        try:
            async with self._connect() as ws:
                await ws.send(json.dumps(["REQ", sub, filters]))
                deadline = time.monotonic() + timeout_s
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(ws.recv(), remaining)
                    except asyncio.TimeoutError:
                        break
                    msg = json.loads(raw)
                    if not isinstance(msg, list) or not msg:
                        continue
                    typ = msg[0]
                    if typ == "EVENT" and len(msg) >= 3 and msg[1] == sub:
                        ev = msg[2]
                        if valid_event(ev):
                            events.append(ev)
                    elif typ == "EOSE" and len(msg) >= 2 and msg[1] == sub:
                        break
                    elif typ == "CLOSED" and len(msg) >= 2 and msg[1] == sub:
                        log.info("%s: oneshot subscription closed: %s", self.url, msg[2:])
                        break
        except Exception as e:
            log.warning("%s: oneshot failed: %r", self.url, e)
        reply_q.put_nowait(events)

    async def _do_batch(self, since: int, floor: int, reply_q: asyncio.Queue):
        sub = "wot-batch"
        cursor: int | None = None
        attempts = 0
        backoff = 1.0
        pages = 0
        total = 0
        while True:
            try:
                async with self._connect() as ws:
                    while True:
                        flt = {"kinds": [3], "since": since, "limit": self.page_limit}
                        if cursor is not None:
                            flt["until"] = cursor
                        await ws.send(json.dumps(["REQ", sub, flt]))
                        page_min, n, closed = await self._read_page(ws, sub)
                        if closed:
                            raise ConnectionError("subscription closed by relay")
                        pages += 1
                        total += n
                        if n == 0 or page_min is None or page_min <= floor:
                            log.info(
                                "%s: batch done: %d pages, %d events, floor=%d",
                                self.url,
                                pages,
                                total,
                                floor,
                            )
                            reply_q.put_nowait(("done", total))
                            return
                        cursor = page_min - 1
                        attempts = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                attempts += 1
                if attempts >= self.max_attempts:
                    log.warning("%s: batch failed after %d attempts: %r", self.url, attempts, e)
                    reply_q.put_nowait(("failed", total))
                    return
                delay = backoff + random.uniform(0, backoff)
                log.info("%s: batch retry %d/%d in %.1fs: %r", self.url, attempts, self.max_attempts, delay, e)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 30)

    async def _read_page(self, ws, sub: str):
        page_min: int | None = None
        n = 0
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), self.eose_timeout_s)
            except asyncio.TimeoutError:
                raise TimeoutError("no message within eose timeout")
            msg = json.loads(raw)
            if not isinstance(msg, list) or not msg:
                continue
            typ = msg[0]
            if typ == "EOSE" and len(msg) >= 2 and msg[1] == sub:
                return page_min, n, False
            if typ == "EVENT" and len(msg) >= 3 and msg[1] == sub:
                ev = msg[2]
                if not valid_event(ev):
                    continue
                ts = ev["created_at"]
                if page_min is None or ts < page_min:
                    page_min = ts
                self.on_event(self.url, ev)
                n += 1
            elif typ == "CLOSED" and len(msg) >= 2 and msg[1] == sub:
                log.info("%s: batch subscription closed: %s", self.url, msg[2:])
                return page_min, n, True
            elif typ == "NOTICE":
                log.debug("%s: notice: %s", self.url, msg[1:] if len(msg) > 1 else msg)

    async def _do_live(self, since: int):
        sub = "wot-live"
        last_ts = since
        backoff = 1.0
        while not self.stopping:
            try:
                async with self._connect() as ws:
                    backoff = 1.0
                    flt = {"kinds": [3], "since": last_ts, "limit": self.page_limit}
                    await ws.send(json.dumps(["REQ", sub, flt]))
                    caught_up = False
                    while True:
                        raw = await ws.recv()
                        msg = json.loads(raw)
                        if not isinstance(msg, list) or not msg:
                            continue
                        typ = msg[0]
                        if typ == "EVENT" and len(msg) >= 3 and msg[1] == sub:
                            ev = msg[2]
                            if valid_event(ev):
                                ts = ev["created_at"]
                                if ts > last_ts:
                                    last_ts = ts
                                self.on_event(self.url, ev)
                        elif typ == "EOSE" and len(msg) >= 2 and msg[1] == sub:
                            if not caught_up:
                                caught_up = True
                                log.info("%s: live stream caught up", self.url)
                        elif typ == "CLOSED" and len(msg) >= 2 and msg[1] == sub:
                            log.info("%s: live subscription closed: %s", self.url, msg[2:])
                            break
                        elif typ == "NOTICE":
                            log.debug("%s: notice: %s", self.url, msg[1:] if len(msg) > 1 else msg)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self.stopping:
                    return
                delay = backoff + random.uniform(0, backoff)
                log.info("%s: live reconnect in %.1fs: %r", self.url, delay, e)
                await asyncio.sleep(delay)
                backoff = min(backoff * 2, 60)
                last_ts = max(last_ts - 60, 0)