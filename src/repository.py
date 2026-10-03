import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError
from .quota import (
    RECORD_COMMITTED,
    RECORD_ROLLED_BACK,
    SCOPE_GATE,
    SCOPE_VENUE,
    SCOPE_ZONE,
    account_available,
    admission_fits,
    effective_zone_capacity,
    gate_capacity,
    venue_capacity,
)
from uuid import uuid4


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS quota_accounts (
                    scope TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    capacity INTEGER,
                    released INTEGER NOT NULL DEFAULT 0,
                    reserved INTEGER NOT NULL DEFAULT 0,
                    version INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(scope, scope_id)
                );
                CREATE TABLE IF NOT EXISTS admission_records (
                    id TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    idem_key TEXT,
                    venue_id TEXT NOT NULL,
                    zone_id TEXT NOT NULL,
                    gate_id TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_admission_idem
                    ON admission_records(actor_id, idem_key);
                CREATE INDEX IF NOT EXISTS idx_admission_gate
                    ON admission_records(gate_id, status);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    # ------------------------------------------------------------------
    # 额度账（quota ledger）
    # ------------------------------------------------------------------
    @staticmethod
    def _account_from_row(row):
        return {
            "scope": row["scope"],
            "scope_id": row["scope_id"],
            "capacity": row["capacity"],
            "released": int(row["released"]),
            "reserved": int(row["reserved"]),
            "version": int(row["version"]),
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _admission_from_row(row):
        return {
            "id": row["id"],
            "actor_id": row["actor_id"],
            "idem_key": row["idem_key"],
            "venue_id": row["venue_id"],
            "zone_id": row["zone_id"],
            "gate_id": row["gate_id"],
            "count": int(row["count"]),
            "status": row["status"],
            "created_at": row["created_at"],
        }

    def get_quota_account(self, scope, scope_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM quota_accounts WHERE scope = ? AND scope_id = ?",
                (scope, scope_id),
            ).fetchone()
        return self._account_from_row(row) if row else None

    def list_quota_accounts(self, venue_id=None):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM quota_accounts ORDER BY scope, scope_id"
            ).fetchall()
        accounts = [self._account_from_row(row) for row in rows]
        if venue_id is not None:
            zone_ids = {
                z["id"]
                for z in self.list_entities(kind="zone")
                if z["data"].get("venue_id") == venue_id
            }
            gate_ids = {
                g["id"]
                for g in self.list_entities(kind="gate")
                if g["data"].get("venue_id") == venue_id
            }
            keep = {("venue", venue_id)} | {("zone", z) for z in zone_ids} | {("gate", g) for g in gate_ids}
            accounts = [a for a in accounts if (a["scope"], a["scope_id"]) in keep]
        return accounts

    def get_admission_record(self, record_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM admission_records WHERE id = ?", (record_id,)
            ).fetchone()
        return self._admission_from_row(row) if row else None

    def find_admission_by_idem(self, actor_id, idem_key):
        if not idem_key:
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM admission_records WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return self._admission_from_row(row) if row else None

    def _ensure_quota_account(self, connection, scope, scope_id, capacity=None):
        connection.execute(
            "INSERT OR IGNORE INTO quota_accounts(scope, scope_id, capacity, released, reserved, version, updated_at) "
            "VALUES (?, ?, ?, 0, 0, 0, ?)",
            (scope, scope_id, capacity, utcnow()),
        )

    def upsert_quota_account(self, scope, scope_id, capacity=None, released=None):
        """创建/更新额度账户（保留既有已放行与预留余额）。"""
        now = utcnow()
        with self._connect() as connection:
            self._ensure_quota_account(connection, scope, scope_id, capacity=capacity)
            if released is None:
                connection.execute(
                    "UPDATE quota_accounts SET capacity = ?, updated_at = ? "
                    "WHERE scope = ? AND scope_id = ?",
                    (capacity, now, scope, scope_id),
                )
            else:
                connection.execute(
                    "UPDATE quota_accounts SET capacity = ?, released = ?, updated_at = ? "
                    "WHERE scope = ? AND scope_id = ?",
                    (capacity, released, now, scope, scope_id),
                )
        return self.get_quota_account(scope, scope_id)

    def _read_account_for_update(self, connection, scope, scope_id):
        row = connection.execute(
            "SELECT * FROM quota_accounts WHERE scope = ? AND scope_id = ?",
            (scope, scope_id),
        ).fetchone()
        return self._account_from_row(row) if row else None

    def commit_admission(
        self,
        *,
        venue_id,
        zone_id,
        gate_id,
        count,
        actor_id,
        idem_key,
        expected_versions,
        entity_id,
        expected_entity_version,
        next_status,
        entity_patch,
    ):
        """原子放行：三道额度一起扣，放行记录与区域人数在同一事务里落账。

        任何一道额度不够、或乐观锁版本对不上，整笔回滚（扣掉的额度和
        放行记录一起退回），由上层按最新余量重试。
        """
        now = utcnow()
        scopes = (
            (SCOPE_VENUE, venue_id, None),
            (SCOPE_ZONE, zone_id, None),
            (SCOPE_GATE, gate_id, None),
        )
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")

            # 幂等重放：同一批人（同一幂等键）只记一遍
            if idem_key:
                row = connection.execute(
                    "SELECT * FROM admission_records WHERE actor_id = ? AND idem_key = ?",
                    (actor_id, idem_key),
                ).fetchone()
                if row and row["status"] == RECORD_COMMITTED:
                    connection.commit()
                    return self._admission_from_row(row)

            # 确保三道额度账户都存在（场馆/进场口可能还没配置上限）
            for scope, scope_id, _ in scopes:
                self._ensure_quota_account(connection, scope, scope_id)

            # 在锁内读取最新余额，并用锁前读到的版本做乐观锁比对
            accounts = {}
            for scope, scope_id, _ in scopes:
                accounts[scope] = self._read_account_for_update(connection, scope, scope_id)

            for scope, scope_id, _ in scopes:
                account = accounts[scope]
                expected = expected_versions.get(scope)
                if expected is not None and int(account["version"]) != int(expected):
                    raise ConflictError("quota changed during admission, please retry")

            # 三道额度一起算，哪一道不够就整笔拒绝
            for scope, scope_id, _ in scopes:
                account = accounts[scope]
                if not admission_fits(
                    account["capacity"], account["released"], account["reserved"], count
                ):
                    available = account_available(
                        account["capacity"], account["released"], account["reserved"]
                    )
                    raise ConflictError(
                        "quota exhausted for %s: available %s, requested %s"
                        % (scope, available, count)
                    )

            # 三道额度一起扣（已放行 += count）
            for scope, scope_id, _ in scopes:
                account = accounts[scope]
                cur = connection.execute(
                    "UPDATE quota_accounts "
                    "SET released = released + ?, version = version + 1, updated_at = ? "
                    "WHERE scope = ? AND scope_id = ? AND version = ?",
                    (count, now, scope, scope_id, account["version"]),
                )
                if cur.rowcount != 1:
                    raise ConflictError("quota changed during admission, please retry")

            # 放行记录落账（同一幂等键重试失败后再提交时，把回滚记录重新置为已放行）
            record_id = uuid4().hex
            connection.execute(
                "INSERT INTO admission_records(id, actor_id, idem_key, venue_id, zone_id, gate_id, "
                "count, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(actor_id, idem_key) DO UPDATE SET "
                "count = excluded.count, status = excluded.status, venue_id = excluded.venue_id, "
                "zone_id = excluded.zone_id, gate_id = excluded.gate_id",
                (
                    record_id,
                    actor_id,
                    idem_key,
                    venue_id,
                    zone_id,
                    gate_id,
                    count,
                    RECORD_COMMITTED,
                    now,
                ),
            )

            # 区域人数与放行同事务更新
            payload = json.dumps(entity_patch, ensure_ascii=False, sort_keys=True)
            cur = connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (next_status, payload, now, entity_id, expected_entity_version),
            )
            if cur.rowcount != 1:
                raise ConflictError("entity changed during admission, please retry")

            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

        if idem_key:
            return self.find_admission_by_idem(actor_id, idem_key)
        return self.get_admission_record(record_id)

    def rollback_admission(self, record_id):
        """整笔退回：把这笔放行扣掉的三道额度和放行记录一起退回。

        用于补偿/冲正。只有 committed 的记录需要退回；区域占用同步减回，
        保证总账重新对上。
        """
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM admission_records WHERE id = ?", (record_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("admission record not found: " + record_id)
            record = self._admission_from_row(row)
            if record["status"] != RECORD_COMMITTED:
                connection.commit()
                return record

            count = record["count"]
            for scope, scope_id in (
                (SCOPE_VENUE, record["venue_id"]),
                (SCOPE_ZONE, record["zone_id"]),
                (SCOPE_GATE, record["gate_id"]),
            ):
                self._ensure_quota_account(connection, scope, scope_id)
                cur = connection.execute(
                    "UPDATE quota_accounts "
                    "SET released = released - ?, version = version + 1, updated_at = ? "
                    "WHERE scope = ? AND scope_id = ?",
                    (count, now, scope, scope_id),
                )
                if cur.rowcount != 1:
                    raise ConflictError("quota account missing during rollback")

            # 区域占用同步减回
            zone_row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (record["zone_id"],)
            ).fetchone()
            if zone_row:
                zone = self._entity_from_row(zone_row)
                data = dict(zone["data"])
                current = int(data.get("current_occupancy", 0))
                data["current_occupancy"] = max(0, current - count)
                payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
                connection.execute(
                    "UPDATE entities SET data = ?, updated_at = ? WHERE id = ?",
                    (payload, now, record["zone_id"]),
                )

            connection.execute(
                "UPDATE admission_records SET status = ? WHERE id = ?",
                (RECORD_ROLLED_BACK, record_id),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_admission_record(record_id)

    def recalculate_quotas(self, venue_id=None):
        """状态一变，预留额度就重算。

        按最新实体状态重算三道额度的容量与已放行余额，并把还没放行的
        预留额度（reserved）释放。区域 evacuating/closed 时容量回 0；
        事件降级或区域恢复后也按最新状态回算。
        """
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            venues = [
                v
                for v in self.list_entities(kind="venue")
                if venue_id is None or v["id"] == venue_id
            ]
            zones = [
                z
                for z in self.list_entities(kind="zone")
                if venue_id is None or z["data"].get("venue_id") == venue_id
            ]
            gates = [
                g
                for g in self.list_entities(kind="gate")
                if venue_id is None or g["data"].get("venue_id") == venue_id
            ]

            def upsert(scope, scope_id, capacity, released):
                self._ensure_quota_account(connection, scope, scope_id, capacity=capacity)
                connection.execute(
                    "UPDATE quota_accounts SET capacity = ?, released = ?, reserved = 0, "
                    "updated_at = ? WHERE scope = ? AND scope_id = ?",
                    (capacity, released, now, scope, scope_id),
                )

            for venue in venues:
                released = sum(
                    int(z["data"].get("current_occupancy", 0))
                    for z in zones
                    if z["data"].get("venue_id") == venue["id"]
                )
                upsert(SCOPE_VENUE, venue["id"], venue_capacity(venue), released)

            for zone in zones:
                released = int(zone["data"].get("current_occupancy", 0))
                upsert(SCOPE_ZONE, zone["id"], effective_zone_capacity(zone), released)

            for gate in gates:
                released = sum(
                    int(r["count"])
                    for r in self._committed_records(connection, gate["id"])
                )
                upsert(SCOPE_GATE, gate["id"], gate_capacity(gate), released)

            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _committed_records(self, connection, gate_id):
        rows = connection.execute(
            "SELECT count FROM admission_records WHERE gate_id = ? AND status = ?",
            (gate_id, RECORD_COMMITTED),
        ).fetchall()
        return [dict(row) for row in rows]

    def backfill_quotas(self):
        """旧数据升级：按现有占用回填初始额度，保证总账对上。"""
        self.recalculate_quotas()
        return self.quota_overview()

    def quota_overview(self, venue_id=None):
        """额度账总览 + 对账（zone 占用 / venue 合计 / gate 放行三处对平）。"""
        zones = [
            z for z in self.list_entities(kind="zone")
            if venue_id is None or z["data"].get("venue_id") == venue_id
        ]
        venues = [
            v for v in self.list_entities(kind="venue")
            if venue_id is None or v["id"] == venue_id
        ]
        gates = [
            g for g in self.list_entities(kind="gate")
            if venue_id is None or g["data"].get("venue_id") == venue_id
        ]
        accounts = self.list_quota_accounts(venue_id)
        by_key = {(a["scope"], a["scope_id"]): a for a in accounts}

        items = []
        balanced = True

        def add(scope, scope_id, capacity, released):
            nonlocal balanced
            account = by_key.get((scope, scope_id))
            reserved = int(account["reserved"]) if account else 0
            available = account_available(capacity, released, reserved)
            entry = {
                "scope": scope,
                "scope_id": scope_id,
                "capacity": capacity,
                "released": released,
                "reserved": reserved,
                "available": available,
                "balanced": available is None or available >= 0,
            }
            if not entry["balanced"]:
                balanced = False
            items.append(entry)

        for venue in venues:
            released = sum(
                int(z["data"].get("current_occupancy", 0))
                for z in zones
                if z["data"].get("venue_id") == venue["id"]
            )
            add(SCOPE_VENUE, venue["id"], venue_capacity(venue), released)

        for zone in zones:
            released = int(zone["data"].get("current_occupancy", 0))
            add(SCOPE_ZONE, zone["id"], effective_zone_capacity(zone), released)
            account = by_key.get((SCOPE_ZONE, zone["id"]))
            if account and int(account["released"]) != released:
                balanced = False

        with self._connect() as connection:
            for gate in gates:
                released = sum(
                    int(r["count"])
                    for r in self._committed_records(connection, gate["id"])
                )
                add(SCOPE_GATE, gate["id"], gate_capacity(gate), released)
                account = by_key.get((SCOPE_GATE, gate["id"]))
                if account and int(account["released"]) != released:
                    balanced = False

        return {"balanced": balanced, "accounts": items}

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
