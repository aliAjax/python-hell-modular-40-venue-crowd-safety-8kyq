"""额度账（quota ledger）的纯计算规则。

放行（zone.admit）要同时受三道额度约束，三道一起扣、哪一道不够就整笔拒绝：

1. 区域额度 zone   —— 区域容量（limited 状态下用 admit_limit）
2. 场馆额度 venue  —— 场馆总容量（capacity_limit）
3. 进场口额度 gate —— 放行上限（flow_limit）

额度账以 `quota_accounts` 记录每个对象的 capacity / released / reserved，
以 `admission_records` 记录每一笔放行。本模块只放无副作用的计算，
持久化与事务在 `repository.py`。
"""

SCOPE_VENUE = "venue"
SCOPE_ZONE = "zone"
SCOPE_GATE = "gate"

RECORD_COMMITTED = "committed"
RECORD_ROLLED_BACK = "rolled_back"

# 未设置上限时视为不限额（场馆总容量 / 进场口放行上限都可能尚未配置）
UNLIMITED = None


def effective_zone_capacity(zone):
    """区域当前可放行的容量。

    - evacuating / closed：不再放人，容量为 0
    - limited：用限流上限 admit_limit（若未配置则退回 capacity）
    - 其它：用区域设计容量 capacity
    """
    status = zone["status"]
    data = zone["data"]
    if status in ("evacuating", "closed"):
        return 0
    if status == "limited":
        limit = data.get("admit_limit")
        if limit is not None:
            return int(limit)
    return int(data.get("capacity", 0))


def venue_capacity(venue):
    """场馆总容量；未配置 capacity_limit 时不限额。"""
    limit = venue["data"].get("capacity_limit")
    return int(limit) if limit is not None else UNLIMITED


def gate_capacity(gate):
    """进场口放行上限；未配置 flow_limit 时不限额。"""
    limit = gate["data"].get("flow_limit")
    return int(limit) if limit is not None else UNLIMITED


def account_available(capacity, released, reserved):
    """可用额度 = 容量 - 已放行 - 预留；容量不限时返回 None。"""
    if capacity is None:
        return None
    return int(capacity) - int(released) - int(reserved)


def account_is_balanced(capacity, released, reserved):
    """额度账不允许透支（可用额度非负）。"""
    available = account_available(capacity, released, reserved)
    return available is None or available >= 0


def admission_fits(capacity, released, reserved, count):
    """这一笔 count 能否在该额度上放行。"""
    available = account_available(capacity, released, reserved)
    return available is None or int(available) >= int(count)
