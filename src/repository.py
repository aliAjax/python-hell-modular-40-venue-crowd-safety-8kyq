import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


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
                    account_id TEXT PRIMARY KEY,
                    scope TEXT NOT NULL,
                    ref_id TEXT NOT NULL,
                    limit_amount INTEGER NOT NULL,
                    used_amount INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS quota_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    batch_id TEXT,
                    delta_limit INTEGER NOT NULL DEFAULT 0,
                    delta_used INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL,
                    ref_id TEXT,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_quota_entries_account
                    ON quota_entries(account_id, id);
                CREATE INDEX IF NOT EXISTS idx_quota_entries_batch
                    ON quota_entries(batch_id);
                CREATE TABLE IF NOT EXISTS admission_batches (
                    batch_id TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    zone_id TEXT NOT NULL,
                    gate_id TEXT NOT NULL,
                    venue_id TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

    @contextmanager
    def transaction(self):
        """单事务边界：额度扣减、流水、批次和实体更新一起提交或一起回滚。"""
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

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

    @staticmethod
    def _account_from_row(row):
        return {
            "account_id": row["account_id"],
            "scope": row["scope"],
            "ref_id": row["ref_id"],
            "limit_amount": int(row["limit_amount"]),
            "used_amount": int(row["used_amount"]),
            "version": int(row["version"]),
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _entry_from_row(row):
        return {
            "id": row["id"],
            "account_id": row["account_id"],
            "batch_id": row["batch_id"],
            "delta_limit": int(row["delta_limit"]),
            "delta_used": int(row["delta_used"]),
            "reason": row["reason"],
            "ref_id": row["ref_id"],
            "detail": json.loads(row["detail"]),
            "created_at": row["created_at"],
        }

    # ---- 事务内读写（连接由调用方的事务提供） ----

    def get_entity_tx(self, connection, entity_id):
        row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities_tx(self, connection, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = connection.execute(
            "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
        ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities_tx(self, connection, kind, field, value):
        return [
            entity
            for entity in self.list_entities_tx(connection, kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def create_entity_tx(self, connection, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
            (entity_id, kind, status, payload, actor_id, now, now),
        )
        return self.get_entity_tx(connection, entity_id)

    def update_entity_tx(self, connection, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
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
        return self.get_entity_tx(connection, entity_id)

    def append_audit_tx(self, connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
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

    def get_idempotency_tx(self, connection, actor_id, idem_key):
        row = connection.execute(
            "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
            (actor_id, idem_key),
        ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency_tx(self, connection, actor_id, idem_key, entity_id):
        connection.execute(
            "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
            "VALUES (?, ?, ?, ?)",
            (actor_id, idem_key, entity_id, utcnow()),
        )

    def get_account_tx(self, connection, account_id):
        row = connection.execute(
            "SELECT * FROM quota_accounts WHERE account_id = ?", (account_id,)
        ).fetchone()
        return self._account_from_row(row) if row else None

    def upsert_account_tx(self, connection, account_id, scope, ref_id, limit_amount, used_amount):
        connection.execute(
            "INSERT INTO quota_accounts(account_id, scope, ref_id, limit_amount, used_amount, version, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 1, ?) "
            "ON CONFLICT(account_id) DO UPDATE SET "
            "limit_amount = excluded.limit_amount, used_amount = excluded.used_amount, "
            "version = quota_accounts.version + 1, updated_at = excluded.updated_at",
            (account_id, scope, ref_id, int(limit_amount), int(used_amount), utcnow()),
        )
        return self.get_account_tx(connection, account_id)

    def insert_entry_tx(self, connection, account_id, batch_id, delta_limit, delta_used, reason, ref_id, detail=None):
        connection.execute(
            "INSERT INTO quota_entries(account_id, batch_id, delta_limit, delta_used, reason, ref_id, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                account_id,
                batch_id,
                int(delta_limit),
                int(delta_used),
                reason,
                ref_id,
                json.dumps(detail or {}, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def get_batch_tx(self, connection, batch_id):
        row = connection.execute(
            "SELECT * FROM admission_batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return dict(row) if row else None

    def insert_batch_tx(self, connection, batch_id, actor_id, zone_id, gate_id, venue_id, count):
        connection.execute(
            "INSERT INTO admission_batches(batch_id, actor_id, zone_id, gate_id, venue_id, count, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (batch_id, actor_id, zone_id, gate_id, venue_id, int(count), utcnow()),
        )

    # ---- 事务外单操作读写 ----

    def create_entity(self, entity_id, kind, status, data, actor_id):
        with self.transaction() as connection:
            self.create_entity_tx(connection, entity_id, kind, status, data, actor_id)
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            return self.get_entity_tx(connection, entity_id)

    def list_entities(self, kind=None, status=None):
        with self._connect() as connection:
            return self.list_entities_tx(connection, kind=kind, status=status)

    def find_entities(self, kind, field, value):
        with self._connect() as connection:
            return self.find_entities_tx(connection, kind, field, value)

    def update_entity(self, entity_id, expected_version, status, data):
        with self.transaction() as connection:
            self.update_entity_tx(connection, entity_id, expected_version, status, data)
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self.transaction() as connection:
            self.append_audit_tx(
                connection, entity_id, actor_id, actor_role, action, from_status, to_status, detail
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
            return self.get_idempotency_tx(connection, actor_id, idem_key)

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self.transaction() as connection:
            self.save_idempotency_tx(connection, actor_id, idem_key, entity_id)

    def get_account(self, account_id):
        with self._connect() as connection:
            return self.get_account_tx(connection, account_id)

    def list_accounts(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM quota_accounts ORDER BY account_id"
            ).fetchall()
        return [self._account_from_row(row) for row in rows]

    def list_entries(self, account_id=None, batch_id=None):
        clauses = []
        params = []
        if account_id:
            clauses.append("account_id = ?")
            params.append(account_id)
        if batch_id:
            clauses.append("batch_id = ?")
            params.append(batch_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM quota_entries" + where + " ORDER BY id", params
            ).fetchall()
        return [self._entry_from_row(row) for row in rows]

    def get_batch(self, batch_id):
        with self._connect() as connection:
            return self.get_batch_tx(connection, batch_id)

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
