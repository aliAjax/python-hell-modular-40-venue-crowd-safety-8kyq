import sqlite3
import time
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, InvalidTransition, NotFoundError
from .rules import (
    ACTIVE_INCIDENT_STATUSES,
    RuleEngine,
    gate_quota_limit,
    venue_quota_limit,
    zone_quota_limit,
)

# 放行写冲突时的重试次数：后到的请求按最新余量重来。
ADMIT_MAX_ATTEMPTS = 5


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    @staticmethod
    def _account_id(scope, ref_id):
        return "%s:%s" % (scope, ref_id)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ---- 创建 ----

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
        with self.repository.transaction() as connection:
            if idempotency_key:
                existing = self.repository.get_idempotency_tx(
                    connection, actor.user_id, idempotency_key
                )
                if existing:
                    entity = self.repository.get_entity_tx(connection, existing)
                    if entity:
                        return entity
            entity = self.repository.create_entity_tx(
                connection, entity_id, kind, status, payload, actor.user_id
            )
            self._quota_on_create(connection, entity)
            self.repository.append_audit_tx(
                connection, entity_id, actor.user_id, actor.role,
                "create", None, status, {"kind": kind},
            )
            if idempotency_key:
                self.repository.save_idempotency_tx(
                    connection, actor.user_id, idempotency_key, entity_id
                )
        return entity

    # ---- 状态变更 ----

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        kind = self.rules.normalize_kind(entity["kind"])
        data = dict(data or {})
        if kind == "zone" and action == "admit":
            return self._admit(actor, entity, data, expected_version)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(actor, entity, action, data, self._lookup)
        merged = dict(entity["data"])
        merged.update(patch)
        with self.repository.transaction() as connection:
            updated = self.repository.update_entity_tx(
                connection, entity_id, expected, next_status, merged
            )
            # 区域/场馆/进场口/事件状态一变，对应额度账户在同一事务里重算。
            self._quota_on_transition(connection, kind, action, updated)
            self.repository.append_audit_tx(
                connection, entity_id, actor.user_id, actor.role,
                action, entity["status"], updated["status"], {"patch": patch},
            )
        return updated

    # ---- 放行：三道额度一起扣 ----

    def _admit(self, actor, zone, data, expected_version):
        idem_key = data.pop("idempotency_key", None)
        # 角色、必填、状态机、闸口校验（与状态无关的部分在事务外完成）。
        _, patch = self.rules.validate_transition(actor, zone, "admit", data, self._lookup)
        count = int(data.get("count"))
        gate_id = data.get("gate_id")
        venue_id = zone["data"].get("venue_id")
        # 失败后重试沿用同一个幂等键，同一批人不会记两遍。
        batch_id = "%s:%s" % (actor.user_id, idem_key) if idem_key else "auto:%s" % uuid4()
        expected = int(expected_version) if expected_version is not None else None
        for attempt in range(ADMIT_MAX_ATTEMPTS):
            try:
                return self._admit_once(
                    actor, zone["id"], gate_id, venue_id, count, batch_id, patch, expected
                )
            except sqlite3.OperationalError:
                # 并发放行写冲突：后到的按最新余量重来。
                if attempt + 1 >= ADMIT_MAX_ATTEMPTS:
                    raise ConflictError("admission contention, please retry")
                time.sleep(0.05 * (attempt + 1))

    def _admit_once(self, actor, zone_id, gate_id, venue_id, count, batch_id, patch, expected):
        with self.repository.transaction() as connection:
            # 幂等：同一批次已记账就直接返回原结果，不重复扣额度。
            batch = self.repository.get_batch_tx(connection, batch_id)
            if batch:
                zone = self.repository.get_entity_tx(connection, zone_id)
                return self._admit_result(zone, batch_id, batch["count"], replayed=True)
            zone = self.repository.get_entity_tx(connection, zone_id)
            if not zone:
                raise NotFoundError("entity not found: " + zone_id)
            if zone["status"] not in ("open", "limited"):
                raise InvalidTransition("cannot admit from status %s" % zone["status"])
            gate = self.repository.get_entity_tx(connection, gate_id)
            if not gate or gate["status"] not in ("open", "restricted"):
                raise ConflictError("entry gate is not open")
            venue = self.repository.get_entity_tx(connection, venue_id)
            if not venue:
                raise NotFoundError("venue not found: " + str(venue_id))
            # 三道额度账户：区域余量、场馆总容量、进场口放行上限（旧数据缺失就按现状补齐）。
            ledgers = [
                self._sync_zone_account(connection, zone, "backfill"),
                self._sync_venue_account(connection, venue, "backfill"),
                self._sync_gate_account(connection, gate, "backfill"),
            ]
            # 哪一道容量不够就整笔拒绝，事务回滚，一道都不扣。
            shortages = []
            for account, limit in ledgers:
                remaining = limit - account["used_amount"]
                if remaining < count:
                    shortages.append(
                        "%s quota remaining %s" % (account["scope"], remaining)
                    )
            if shortages:
                raise ConflictError(
                    "admission of %s rejected: %s" % (count, "; ".join(shortages))
                )
            quotas = {}
            for account, limit in ledgers:
                new_used = account["used_amount"] + count
                self.repository.upsert_account_tx(
                    connection, account["account_id"], account["scope"],
                    account["ref_id"], limit, new_used,
                )
                self.repository.insert_entry_tx(
                    connection, account["account_id"], batch_id, 0, count,
                    "admit", account["ref_id"],
                    {"count": count, "limit": limit, "used": new_used},
                )
                quotas[account["scope"]] = {"limit": limit, "used": new_used}
            self.repository.insert_batch_tx(
                connection, batch_id, actor.user_id, zone_id, gate_id, venue_id, count
            )
            merged = dict(zone["data"])
            merged.update(patch)
            merged["current_occupancy"] = int(zone["data"].get("current_occupancy", 0)) + count
            # 放行不改变区域状态（limited区域保持limited）；版本不符则整笔回滚。
            updated = self.repository.update_entity_tx(
                connection, zone_id, expected, zone["status"], merged
            )
            self.repository.append_audit_tx(
                connection, zone_id, actor.user_id, actor.role,
                "admit", zone["status"], updated["status"],
                {"patch": patch, "admission": {
                    "batch_id": batch_id, "count": count,
                    "gate_id": gate_id, "quotas": quotas,
                }},
            )
        return self._admit_result(updated, batch_id, count, replayed=False)

    @staticmethod
    def _admit_result(zone, batch_id, count, replayed):
        result = dict(zone)
        result["admission"] = {"batch_id": batch_id, "count": count, "replayed": replayed}
        return result

    # ---- 额度账户同步 ----

    def _sync_zone_account(self, connection, zone, created_reason):
        account_id = self._account_id("zone", zone["id"])
        incidents = [
            item
            for item in self.repository.find_entities_tx(connection, "incident", "zone_id", zone["id"])
            if item["status"] in ACTIVE_INCIDENT_STATUSES
        ]
        limit = zone_quota_limit(zone, incidents)
        account = self.repository.get_account_tx(connection, account_id)
        if account is None:
            used = int(zone["data"].get("current_occupancy", 0))
            account = self.repository.upsert_account_tx(
                connection, account_id, "zone", zone["id"], limit, used
            )
            self.repository.insert_entry_tx(
                connection, account_id, None, limit, used,
                created_reason, zone["id"], {"limit": limit, "used": used},
            )
        elif account["limit_amount"] != limit:
            account = self._recalc_account(connection, account, limit)
        return account, limit

    def _sync_venue_account(self, connection, venue, created_reason):
        account_id = self._account_id("venue", venue["id"])
        zones = self.repository.find_entities_tx(connection, "zone", "venue_id", venue["id"])
        limit = venue_quota_limit(venue, zones)
        account = self.repository.get_account_tx(connection, account_id)
        if account is None:
            used = sum(int(item["data"].get("current_occupancy", 0)) for item in zones)
            account = self.repository.upsert_account_tx(
                connection, account_id, "venue", venue["id"], limit, used
            )
            self.repository.insert_entry_tx(
                connection, account_id, None, limit, used,
                created_reason, venue["id"], {"limit": limit, "used": used},
            )
        elif account["limit_amount"] != limit:
            account = self._recalc_account(connection, account, limit)
        return account, limit

    def _sync_gate_account(self, connection, gate, created_reason, reset_used=False):
        account_id = self._account_id("gate", gate["id"])
        limit = gate_quota_limit(gate)
        account = self.repository.get_account_tx(connection, account_id)
        if account is None:
            account = self.repository.upsert_account_tx(
                connection, account_id, "gate", gate["id"], limit, 0
            )
            self.repository.insert_entry_tx(
                connection, account_id, None, limit, 0,
                created_reason, gate["id"], {"limit": limit, "used": 0},
            )
            return account, limit
        used = account["used_amount"]
        if reset_used and used:
            # 进场口重新开放：上一个放行窗口的已用量清零。
            self.repository.insert_entry_tx(
                connection, account_id, None, 0, -used,
                "reset", gate["id"], {"used": used},
            )
            used = 0
        if account["limit_amount"] != limit or used != account["used_amount"]:
            if account["limit_amount"] != limit:
                self.repository.insert_entry_tx(
                    connection, account_id, None, limit - account["limit_amount"], 0,
                    "recalc", gate["id"],
                    {"old_limit": account["limit_amount"], "new_limit": limit},
                )
            account = self.repository.upsert_account_tx(
                connection, account_id, "gate", gate["id"], limit, used
            )
        return account, limit

    def _recalc_account(self, connection, account, limit):
        """状态变化后重算预留额度：还没放行的部分随上限下调而释放。"""
        old_limit = account["limit_amount"]
        used = account["used_amount"]
        account = self.repository.upsert_account_tx(
            connection, account["account_id"], account["scope"],
            account["ref_id"], limit, used,
        )
        self.repository.insert_entry_tx(
            connection, account["account_id"], None, limit - old_limit, 0,
            "recalc", account["ref_id"],
            {
                "old_limit": old_limit,
                "new_limit": limit,
                "released": max(0, (old_limit - used) - (limit - used)),
            },
        )
        return account

    def _quota_on_create(self, connection, entity):
        kind = entity["kind"]
        if kind == "zone":
            self._sync_zone_account(connection, entity, "init")
            venue = self.repository.get_entity_tx(connection, entity["data"].get("venue_id"))
            if venue:
                self._sync_venue_account(connection, venue, "backfill")
        elif kind == "venue":
            self._sync_venue_account(connection, entity, "init")
        elif kind == "gate":
            self._sync_gate_account(connection, entity, "init")
        elif kind == "incident":
            zone = self.repository.get_entity_tx(connection, entity["data"].get("zone_id"))
            if zone:
                self._sync_zone_account(connection, zone, "backfill")

    def _quota_on_transition(self, connection, kind, action, updated):
        if kind == "zone":
            self._sync_zone_account(connection, updated, "backfill")
        elif kind == "venue":
            self._sync_venue_account(connection, updated, "backfill")
        elif kind == "gate":
            self._sync_gate_account(
                connection, updated, "backfill", reset_used=(action == "open")
            )
        elif kind == "incident":
            # 事件升降级、结案或重开：按最新状态回算所属区域的预留额度。
            zone = self.repository.get_entity_tx(connection, updated["data"].get("zone_id"))
            if zone:
                self._sync_zone_account(connection, zone, "backfill")

    # ---- 迁移与对账 ----

    def migrate_quotas(self):
        """旧数据升级：按现有占用回填初始额度，保证总账对上。可重复执行。"""
        created = []
        with self.repository.transaction() as connection:
            for zone in self.repository.list_entities_tx(connection, kind="zone"):
                account_id = self._account_id("zone", zone["id"])
                if not self.repository.get_account_tx(connection, account_id):
                    self._sync_zone_account(connection, zone, "backfill")
                    created.append(account_id)
            for venue in self.repository.list_entities_tx(connection, kind="venue"):
                account_id = self._account_id("venue", venue["id"])
                if not self.repository.get_account_tx(connection, account_id):
                    self._sync_venue_account(connection, venue, "backfill")
                    created.append(account_id)
            for gate in self.repository.list_entities_tx(connection, kind="gate"):
                account_id = self._account_id("gate", gate["id"])
                if not self.repository.get_account_tx(connection, account_id):
                    self._sync_gate_account(connection, gate, "backfill")
                    created.append(account_id)
        return {"created": created, "accounts": self.repository.list_accounts()}

    def verify_quota_ledger(self):
        """对账：区域账户已用=区域占用，场馆账户已用=各区域占用之和。"""
        mismatches = []
        occupancy_by_venue = {}
        for zone in self.repository.list_entities(kind="zone"):
            occupancy = int(zone["data"].get("current_occupancy", 0))
            account = self.repository.get_account(self._account_id("zone", zone["id"]))
            if not account:
                mismatches.append("missing zone account: " + zone["id"])
            elif account["used_amount"] != occupancy:
                mismatches.append(
                    "zone %s used %s != occupancy %s"
                    % (zone["id"], account["used_amount"], occupancy)
                )
            venue_id = zone["data"].get("venue_id")
            occupancy_by_venue[venue_id] = occupancy_by_venue.get(venue_id, 0) + occupancy
        for venue in self.repository.list_entities(kind="venue"):
            account = self.repository.get_account(self._account_id("venue", venue["id"]))
            expected = occupancy_by_venue.get(venue["id"], 0)
            if not account:
                mismatches.append("missing venue account: " + venue["id"])
            elif account["used_amount"] != expected:
                mismatches.append(
                    "venue %s used %s != zone occupancy sum %s"
                    % (venue["id"], account["used_amount"], expected)
                )
        return mismatches

    # ---- 查询 ----

    def quota_accounts(self):
        return self.repository.list_accounts()

    def quota_ledger(self, account_id=None, batch_id=None):
        return self.repository.list_entries(account_id=account_id, batch_id=batch_id)

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
