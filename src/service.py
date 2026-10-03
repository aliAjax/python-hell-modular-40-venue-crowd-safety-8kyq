from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError
from .quota import SCOPE_GATE, SCOPE_VENUE, SCOPE_ZONE
from .rules import RuleEngine

# 两个操作员同时提交同一区域放行时，后到的按最新余量重试
ADMIT_MAX_ATTEMPTS = 5


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        if kind == "zone":
            # 区域立账：初始额度 = 区域容量，已放行 0
            self.repository.upsert_quota_account(
                SCOPE_ZONE, entity_id, capacity=int(payload.get("capacity", 0))
            )
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None, idempotency_key=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])

        # 区域放行：走额度账，三道额度一起扣、整笔提交、失败按最新余量重试
        if kind == "zone" and action == "admit":
            return self._admit(actor, entity, data, expected_version, idempotency_key)

        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        # 区域/场馆/进场口/事件状态一变，预留额度就按最新状态重算
        self._recalc_after(kind, action, updated)
        return updated

    def _recalc_after(self, kind, action, entity):
        if kind not in ("venue", "zone", "gate", "incident"):
            return
        venue_id = entity["id"] if kind == "venue" else entity["data"].get("venue_id")
        if venue_id:
            try:
                self.repository.recalculate_quotas(venue_id)
            except Exception:
                # 重算是对账性动作，失败不应掩盖主流程；总账可通过 backfill 修复
                pass

    def _admit(self, actor, entity, data, expected_version, idempotency_key):
        payload = dict(data or {})

        # 先用规则做一次输入校验（count 必须为正整数、gate 必须开放且服务本区域等），
        # 缺失/非法字段在此抛出干净的 ValidationError / ConflictError
        self.rules.validate_transition(actor, entity, "admit", dict(payload), self._lookup)
        count = int(payload.get("count"))
        gate_id = payload.get("gate_id")
        venue_id = entity["data"]["venue_id"]

        # 幂等：同一批人（同一幂等键）只记一遍，失败后重试沿用同一键
        if idempotency_key:
            existing = self.repository.find_admission_by_idem(actor.user_id, idempotency_key)
            if existing and existing["status"] == "committed":
                return self.repository.get_entity(entity["id"])

        current = entity
        last_error = None
        for _ in range(ADMIT_MAX_ATTEMPTS):
            next_status, patch = self.rules.validate_transition(
                actor, current, "admit", dict(payload), self._lookup
            )
            merged = dict(current["data"])
            merged.update(patch)
            expected_entity_version = (
                int(expected_version) if expected_version is not None else current["version"]
            )
            versions = self._quota_versions(venue_id, current["id"], gate_id)
            try:
                self.repository.commit_admission(
                    venue_id=venue_id,
                    zone_id=current["id"],
                    gate_id=gate_id,
                    count=count,
                    actor_id=actor.user_id,
                    idem_key=idempotency_key,
                    expected_versions=versions,
                    entity_id=current["id"],
                    expected_entity_version=expected_entity_version,
                    next_status=next_status,
                    entity_patch=merged,
                )
                self.audit.record(
                    current["id"],
                    actor,
                    "admit",
                    current["status"],
                    next_status,
                    {"gate_id": gate_id, "count": count, "idempotency_key": idempotency_key},
                )
                return self.repository.get_entity(current["id"])
            except ConflictError as exc:
                # 后到的按最新余量重来：重读区域与额度账，重新校验后再提交
                last_error = exc
                fresh = self.repository.get_entity(current["id"])
                if not fresh:
                    raise NotFoundError("entity not found: " + current["id"])
                current = fresh
                expected_version = None
                continue
        raise ConflictError("admission retried %s times: %s" % (ADMIT_MAX_ATTEMPTS, last_error))

    def _quota_versions(self, venue_id, zone_id, gate_id):
        versions = {}
        for scope, scope_id in (
            (SCOPE_VENUE, venue_id),
            (SCOPE_ZONE, zone_id),
            (SCOPE_GATE, gate_id),
        ):
            account = self.repository.get_quota_account(scope, scope_id)
            versions[scope] = account["version"] if account else None
        return versions

    def backfill_quotas(self):
        return self.repository.backfill_quotas()

    def quota_overview(self, venue_id=None):
        return self.repository.quota_overview(venue_id)

    def rollback_admission(self, record_id):
        return self.repository.rollback_admission(record_id)

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
