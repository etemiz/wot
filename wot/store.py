from __future__ import annotations

import struct

import lmdb

_TRU = b"tru"
_PACK = struct.Struct("f").pack


class TruStore:
    def __init__(self, path: str, map_size: int):
        self.env = lmdb.open(path, map_size=map_size, max_dbs=4, subdir=True, create=True)
        self.tru_db = self.env.open_db(_TRU, create=True)

    def write_all(self, pairs) -> int:
        n = 0
        with self.env.begin(write=True) as txn:
            txn.drop(self.tru_db, delete=False)
            put = txn.put
            db = self.tru_db
            for pk, score in pairs:
                put(pk, _PACK(score), db=db)
                n += 1
        return n

    def update(self, changed: dict[bytes, float | None]) -> int:
        n = 0
        with self.env.begin(write=True) as txn:
            put = txn.put
            delete = txn.delete
            db = self.tru_db
            for pk, score in changed.items():
                if score is None:
                    if delete(pk, db=db):
                        n += 1
                else:
                    put(pk, _PACK(score), db=db)
                    n += 1
        return n

    def count(self) -> int:
        with self.env.begin(db=self.tru_db) as txn:
            return txn.stat()["entries"]

    def close(self):
        self.env.close()


def open_read(path: str) -> lmdb.Environment:
    return lmdb.open(path, readonly=True, lock=False, max_dbs=4, subdir=True)


def read_tru(path: str) -> dict[bytes, float]:
    env = open_read(path)
    try:
        db = env.open_db(_TRU)
        out = {}
        unpack = struct.Struct("f").unpack
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            for k, v in cur:
                out[k] = unpack(v)[0]
        return out
    finally:
        env.close()