from __future__ import annotations


class FollowGraph:
    def __init__(self):
        self.id_of: dict[bytes, int] = {}
        self.pubkey_of: list[bytes] = []
        self.follows: list[set[int]] = []
        self.followers: list[set[int]] = []
        self._latest: dict[bytes, tuple[int, bytes, list[bytes]]] = {}
        self.last_event: dict[bytes, tuple[int, bytes]] = {}

    def intern(self, pk: bytes) -> int:
        i = self.id_of.get(pk)
        if i is None:
            i = len(self.pubkey_of)
            self.id_of[pk] = i
            self.pubkey_of.append(pk)
            self.follows.append(set())
            self.followers.append(set())
        return i

    def is_newer(self, author: bytes, created_at: int, event_id: bytes) -> bool:
        key = (created_at, event_id)
        cur = self.last_event.get(author)
        if cur is not None and key <= cur:
            return False
        self.last_event[author] = key
        return True

    def apply_contact_event(
        self, author: bytes, created_at: int, event_id: bytes, p_tags: list[bytes]
    ) -> bool:
        if not self.is_newer(author, created_at, event_id):
            return False
        self._latest[author] = (created_at, event_id, p_tags)
        return True

    def build_edges(self) -> int:
        follows = self.follows
        followers = self.followers
        intern = self.intern
        edges = 0
        for author, (_ts, _eid, pks) in self._latest.items():
            aid = intern(author)
            new = {intern(pk) for pk in pks}
            old = follows[aid]
            if old == new:
                continue
            removed = old - new
            added = new - old
            for u in removed:
                followers[u].discard(aid)
            for u in added:
                followers[u].add(aid)
            follows[aid] = new
            edges += len(added)
        self._latest = {}
        return edges

    def replace_follows(self, author: bytes, pks: list[bytes]) -> set[int]:
        aid = self.intern(author)
        new = {self.intern(pk) for pk in pks}
        old = self.follows[aid]
        removed = old - new
        added = new - old
        followers = self.followers
        for u in removed:
            followers[u].discard(aid)
        for u in added:
            followers[u].add(aid)
        self.follows[aid] = new
        return added | removed