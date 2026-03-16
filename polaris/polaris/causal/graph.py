"""
因果图操作
==========
从 Anchor DB 读取 causal_variables + causal_links，构建内存图，
提供路径查询、链组合、矛盾检测。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from polaris.db.anchor import query_df


# ── 数据结构 ─────────────────────────────────────────────────────────────


@dataclass
class Variable:
    id: int
    name: str
    domain: str
    description: str
    observable: bool


@dataclass
class Link:
    id: int
    cause_id: int
    effect_id: int
    mechanism: str
    magnitude: str | None
    lag: str | None
    conditions: str | None
    confidence: float


@dataclass
class CausalChain:
    """一条因果传导路径：多个 Link 首尾相连。"""
    links: list[Link]

    @property
    def link_ids(self) -> list[int]:
        return [l.id for l in self.links]

    @property
    def cause_id(self) -> int:
        return self.links[0].cause_id

    @property
    def effect_id(self) -> int:
        return self.links[-1].effect_id

    @property
    def min_confidence(self) -> float:
        return min(l.confidence for l in self.links) if self.links else 0.0

    def describe(self, var_map: dict[int, Variable]) -> str:
        parts = []
        for l in self.links:
            cause_name = var_map.get(l.cause_id, Variable(0, "?", "", "", False)).name
            effect_name = var_map.get(l.effect_id, Variable(0, "?", "", "", False)).name
            parts.append(f"{cause_name} →[{l.mechanism}]→ {effect_name}")
        return " → ".join(parts)


# ── 图 ───────────────────────────────────────────────────────────────────


class CausalGraph:
    """内存因果图。从 Anchor DB 加载。"""

    def __init__(self):
        self.variables: dict[int, Variable] = {}      # id → Variable
        self.var_by_name: dict[str, Variable] = {}     # name → Variable
        self.links: dict[int, Link] = {}               # id → Link
        self._outgoing: dict[int, list[Link]] = {}     # cause_id → [Link]
        self._incoming: dict[int, list[Link]] = {}     # effect_id → [Link]

    def load(self) -> None:
        """从 Anchor DB 加载全部变量和因果链。"""
        self._load_variables()
        self._load_links()

    def _load_variables(self):
        df = query_df("SELECT id, name, domain, description, observable FROM causal_variables")
        for _, row in df.iterrows():
            v = Variable(
                id=int(row["id"]),
                name=row["name"],
                domain=row["domain"],
                description=row["description"],
                observable=bool(row["observable"]),
            )
            self.variables[v.id] = v
            self.var_by_name[v.name] = v

    def _load_links(self):
        df = query_df(
            "SELECT id, cause_id, effect_id, mechanism, magnitude, lag, conditions, confidence "
            "FROM causal_links"
        )
        for _, row in df.iterrows():
            l = Link(
                id=int(row["id"]),
                cause_id=int(row["cause_id"]),
                effect_id=int(row["effect_id"]),
                mechanism=row["mechanism"],
                magnitude=row.get("magnitude"),
                lag=row.get("lag"),
                conditions=row.get("conditions"),
                confidence=float(row["confidence"]),
            )
            self.links[l.id] = l
            self._outgoing.setdefault(l.cause_id, []).append(l)
            self._incoming.setdefault(l.effect_id, []).append(l)

    # ── 查询 ─────────────────────────────────────────────────────────

    def downstream(self, variable_id: int, max_depth: int = 5) -> list[CausalChain]:
        """从某变量出发，沿因果方向找所有可达路径。"""
        chains: list[CausalChain] = []
        self._dfs_forward(variable_id, [], set(), max_depth, chains)
        return chains

    def upstream(self, variable_id: int, max_depth: int = 5) -> list[CausalChain]:
        """从某变量回溯，找所有原因路径。"""
        chains: list[CausalChain] = []
        self._dfs_backward(variable_id, [], set(), max_depth, chains)
        # 反转链方向（DFS 是倒着走的）
        for chain in chains:
            chain.links.reverse()
        return chains

    def contradictions(self) -> list[tuple[Link, Link]]:
        """找矛盾：同一对 (cause, effect) 有多条 link，机制可能矛盾。"""
        pairs: dict[tuple[int, int], list[Link]] = {}
        for l in self.links.values():
            key = (l.cause_id, l.effect_id)
            pairs.setdefault(key, []).append(l)
        return [
            (links[i], links[j])
            for links in pairs.values()
            if len(links) > 1
            for i in range(len(links))
            for j in range(i + 1, len(links))
        ]

    # ── DFS ──────────────────────────────────────────────────────────

    def _dfs_forward(self, vid, path, visited, max_depth, result):
        if len(path) > 0:
            result.append(CausalChain(links=list(path)))
        if len(path) >= max_depth:
            return
        for link in self._outgoing.get(vid, []):
            if link.effect_id not in visited:
                visited.add(link.effect_id)
                path.append(link)
                self._dfs_forward(link.effect_id, path, visited, max_depth, result)
                path.pop()
                visited.discard(link.effect_id)

    def _dfs_backward(self, vid, path, visited, max_depth, result):
        if len(path) > 0:
            result.append(CausalChain(links=list(path)))
        if len(path) >= max_depth:
            return
        for link in self._incoming.get(vid, []):
            if link.cause_id not in visited:
                visited.add(link.cause_id)
                path.append(link)
                self._dfs_backward(link.cause_id, path, visited, max_depth, result)
                path.pop()
                visited.discard(link.cause_id)
