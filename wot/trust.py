from __future__ import annotations

import heapq

import wot.graph


def propagate(
    graph: wot.graph.FollowGraph, seeds: dict[int, float], decay: float
) -> list[float]:
    n = len(graph.pubkey_of)
    t = [0.0] * n
    heap = []
    for nid, w in seeds.items():
        if w > t[nid]:
            t[nid] = w
            heap.append((-w, nid))
    heapq.heapify(heap)
    follows = graph.follows
    push = heapq.heappush
    pop = heapq.heappop
    while heap:
        negt, v = pop(heap)
        tv = -negt
        if t[v] > tv:
            continue
        cand = tv * decay
        for u in follows[v]:
            if cand > t[u]:
                t[u] = cand
                push(heap, (-cand, u))
    return t


def propagate_from(
    graph: wot.graph.FollowGraph,
    t: list[float],
    seeds: set[int],
    decay: float,
) -> set[int]:
    heap = []
    for v in seeds:
        if t[v] > 0.0:
            heap.append((-t[v], v))
    heapq.heapify(heap)
    changed = set()
    follows = graph.follows
    push = heapq.heappush
    pop = heapq.heappop
    while heap:
        negt, v = pop(heap)
        tv = -negt
        if t[v] > tv:
            continue
        cand = tv * decay
        for u in follows[v]:
            if cand > t[u]:
                t[u] = cand
                changed.add(u)
                push(heap, (-cand, u))
    return changed


def follower_score_of(
    graph: wot.graph.FollowGraph,
    t: list[float],
    decay: float,
    top_n: int,
    hub_min_follows: int,
    hub_exponent: float,
    u: int,
) -> float | None:
    flw = graph.followers[u]
    if not flw:
        return None
    follows = graph.follows
    weights = []
    for v in flw:
        tv = t[v]
        if tv <= 0.0:
            continue
        fcount = len(follows[v])
        if fcount > hub_min_follows:
            tv *= fcount ** (-hub_exponent)
        weights.append(tv)
    if not weights:
        return None
    weights.sort(reverse=True)
    return sum(weights[:top_n]) / top_n * decay


def follower_scores(
    graph: wot.graph.FollowGraph,
    t: list[float],
    decay: float,
    top_n: int,
    hub_min_follows: int,
    hub_exponent: float,
) -> list[float | None]:
    n = len(graph.pubkey_of)
    res: list[float | None] = [None] * n
    for u in range(n):
        res[u] = follower_score_of(
            graph, t, decay, top_n, hub_min_follows, hub_exponent, u
        )
    return res


def score(
    graph: wot.graph.FollowGraph,
    t: list[float],
    w: list[float | None],
) -> list[tuple[bytes, float]]:
    pk = graph.pubkey_of
    return [(pk[u], w[u]) for u in range(len(t)) if w[u] is not None]