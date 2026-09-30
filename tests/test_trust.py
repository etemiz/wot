import os
import struct
import tempfile
import unittest

from wot import trust
from wot.config import decode_pubkey
from wot.graph import FollowGraph
from wot.store import TruStore, read_tru


def pk(n: int) -> bytes:
    return n.to_bytes(32, "big")


def eid(n: int) -> bytes:
    return n.to_bytes(32, "big")


def add_list(graph, author: bytes, created_at: int, event_id: bytes, follows: list[bytes]):
    return graph.apply_contact_event(author, created_at, event_id, list(follows))


class ConfigTests(unittest.TestCase):
    def test_decode_npub_known(self):
        raw = decode_pubkey("npub1a2cww4kn9wqte4ry70vyfwqyqvpswksna27rtxd8vty6c74era8sdcw83a")
        self.assertEqual(raw.hex(), "eab0e756d32b80bcd464f3d844b8040303075a13eabc3599a762c9ac7ab91f4f")

    def test_decode_hex(self):
        h = "dd664d5e4016433a8cd69f005ae1480804351789b59de5af06276de65633d319"
        self.assertEqual(decode_pubkey(h).hex(), h)

    def test_bad_checksum(self):
        with self.assertRaises(ValueError):
            decode_pubkey("npub1a2cww4kn9wqte4ry70vyfwqyqvpswksna27rtxd8vty6c74era8sdcw83b")


class PropagationTests(unittest.TestCase):
    def build(self):
        g = FollowGraph()
        R, A, B, C, D = pk(1), pk(2), pk(3), pk(4), pk(5)
        add_list(g, R, 100, eid(1), [A, B])
        add_list(g, A, 100, eid(2), [C])
        add_list(g, B, 100, eid(3), [C])
        add_list(g, C, 100, eid(4), [D])
        g.build_edges()
        return g, {1: 0.8}

    def test_propagation_chain(self):
        g, roots = self.build()
        t = trust.propagate(g, {g.id_of[pk(1)]: 0.8}, 0.7)
        self.assertAlmostEqual(t[g.id_of[pk(2)]], 0.56)
        self.assertAlmostEqual(t[g.id_of[pk(3)]], 0.56)
        self.assertAlmostEqual(t[g.id_of[pk(4)]], 0.392)
        self.assertAlmostEqual(t[g.id_of[pk(5)]], 0.2744)
        self.assertEqual(t[g.id_of[pk(1)]], 0.8)

    def test_max_not_sum(self):
        g, _ = self.build()
        t = trust.propagate(g, {g.id_of[pk(1)]: 0.8}, 0.7)
        self.assertAlmostEqual(t[g.id_of[pk(4)]], 0.392)
        self.assertLess(t[g.id_of[pk(4)]], 2 * 0.392)

    def test_scores_golden(self):
        g, _ = self.build()
        t = trust.propagate(g, {g.id_of[pk(1)]: 0.8}, 0.7)
        w = trust.follower_scores(g, t, 0.7, 10, 20, 0.3)
        pairs = dict(trust.score(g, t, w))
        self.assertAlmostEqual(pairs[pk(2)], 0.7 * 0.8 / 10)
        self.assertAlmostEqual(pairs[pk(3)], 0.7 * 0.8 / 10)
        self.assertAlmostEqual(pairs[pk(4)], 0.7 * (0.56 + 0.56) / 10)
        self.assertAlmostEqual(pairs[pk(5)], 0.7 * 0.392 / 10)
        self.assertNotIn(pk(1), pairs)

    def test_hub_penalty(self):
        g = FollowGraph()
        R, A = pk(10), pk(11)
        add_list(g, R, 100, eid(10), [A] + [pk(100 + i) for i in range(24)])
        g.build_edges()
        t = trust.propagate(g, {g.id_of[R]: 0.8}, 0.7)
        w = trust.follower_scores(g, t, 0.7, 10, 20, 0.3)
        pairs = dict(trust.score(g, t, w))
        expected = 0.7 * 0.8 / (25 ** 0.3) / 10
        self.assertAlmostEqual(pairs[A], expected, places=6)

    def test_denominator_always_top_n(self):
        g = FollowGraph()
        R = pk(20)
        followers = [pk(21), pk(22), pk(23)]
        add_list(g, R, 100, eid(20), followers)
        for f in followers:
            add_list(g, f, 100, eid(30 + f[31]), [pk(30)])
        add_list(g, pk(30), 100, eid(40), [])
        g.build_edges()
        t = trust.propagate(g, {g.id_of[R]: 0.8}, 0.7)
        w = trust.follower_scores(g, t, 0.7, 10, 20, 0.3)
        pairs = dict(trust.score(g, t, w))
        self.assertAlmostEqual(pairs[pk(30)], 0.7 * (3 * 0.56) / 10)

    def test_latest_event_wins(self):
        g = FollowGraph()
        A, B, C = pk(40), pk(41), pk(42)
        self.assertTrue(add_list(g, A, 200, eid(50), [B]))
        self.assertFalse(add_list(g, A, 100, eid(51), [C]))
        self.assertTrue(add_list(g, A, 200, eid(52), [B, C]))
        g.build_edges()
        aid = g.id_of[A]
        self.assertEqual(g.follows[aid], {g.id_of[B], g.id_of[C]})

    def test_replace_follows_unfollow(self):
        g = FollowGraph()
        A, B, C = pk(50), pk(51), pk(52)
        add_list(g, A, 100, eid(60), [B, C])
        g.build_edges()
        dirty = g.replace_follows(A, [B])
        aid = g.id_of[A]
        self.assertEqual(g.follows[aid], {g.id_of[B]})
        self.assertNotIn(aid, g.followers[g.id_of[C]])
        self.assertIn(g.id_of[C], dirty)

    def test_determinism(self):
        s1 = self._run_scores()
        s2 = self._run_scores()
        self.assertEqual(s1, s2)

    def _run_scores(self):
        g, roots = self.build()
        t = trust.propagate(g, {g.id_of[pk(1)]: 0.8}, 0.7)
        w = trust.follower_scores(g, t, 0.7, 10, 20, 0.3)
        return trust.score(g, t, w)


class StoreTests(unittest.TestCase):
    def test_roundtrip(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "db")
            store = TruStore(path, 1 << 26)
            pairs = [(pk(i), 0.1 * i) for i in range(1, 11)]
            self.assertEqual(store.write_all(pairs), 10)
            store.close()
            loaded = read_tru(path)
            self.assertEqual(len(loaded), 10)
            for p, v in pairs:
                self.assertAlmostEqual(loaded[p], struct.unpack("f", struct.pack("f", v))[0], places=7)

    def test_update_and_delete(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "db")
            store = TruStore(path, 1 << 26)
            store.write_all([(pk(1), 0.5)])
            store.update({pk(1): 0.9, pk(2): None})
            store.close()
            loaded = read_tru(path)
            self.assertEqual(len(loaded), 1)
            self.assertAlmostEqual(loaded[pk(1)], 0.9, places=7)

    def test_write_all_replaces(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "db")
            store = TruStore(path, 1 << 26)
            store.write_all([(pk(1), 0.5), (pk(2), 0.6)])
            store.write_all([(pk(1), 0.7)])
            store.close()
            loaded = read_tru(path)
            self.assertEqual(len(loaded), 1)
            self.assertAlmostEqual(loaded[pk(1)], 0.7, places=7)


if __name__ == "__main__":
    unittest.main()