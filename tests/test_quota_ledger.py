import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from src.domain import Actor, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class QuotaLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "quota.db"
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        self.coordinator = Actor("commander", "coordinator")
        self.operator = Actor("gate-op", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup(self, venue_capacity=None, zone_capacity=100, gate_flow=None):
        venue = self.service.create(
            self.coordinator, "venue", {"name": "V", "address": "A"}
        )
        zone = self.service.create(
            self.coordinator,
            "zone",
            {"venue_id": venue["id"], "name": "Z", "capacity": zone_capacity},
        )
        gate = self.service.create(
            self.coordinator,
            "gate",
            {"venue_id": venue["id"], "name": "G", "zone_ids": [zone["id"]]},
        )
        if venue_capacity is not None:
            self.service.transition(
                self.coordinator,
                venue["id"],
                "limit",
                {"reason": "safety capacity", "capacity_limit": venue_capacity},
            )
        self.service.transition(self.operator, gate["id"], "open", {"operator_id": "gate-op"})
        if gate_flow is not None:
            # restrict 设下 flow_limit 后再 restore 恢复 open（flow_limit 保留），
            # 这样进场口既可放行又受放行上限约束
            self.service.transition(
                self.coordinator,
                gate["id"],
                "restrict",
                {"reason": "gate flow", "flow_limit": gate_flow},
            )
            self.service.transition(self.operator, gate["id"], "restore", {})
        self.service.transition(self.operator, zone["id"], "open", {"checklist": "clear"})
        return venue, zone, gate

    def _admit(self, zone, gate, count, key=None):
        return self.service.transition(
            self.operator,
            zone["id"],
            "admit",
            {"gate_id": gate["id"], "count": count, "admitted_at": "t"},
            idempotency_key=key,
        )

    def _raw(self, sql, params=()):
        conn = sqlite3.connect(str(self.db_path))
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def test_three_ledgers_deducted_together(self):
        venue, zone, gate = self._setup(venue_capacity=100, zone_capacity=100, gate_flow=100)
        self._admit(zone, gate, 40)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])
        accounts = {(a["scope"], a["scope_id"]): a for a in overview["accounts"]}
        self.assertEqual(accounts[("zone", zone["id"])]["released"], 40)
        self.assertEqual(accounts[("venue", venue["id"])]["released"], 40)
        self.assertEqual(accounts[("gate", gate["id"])]["released"], 40)
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 40)

    def test_reject_when_venue_capacity_exceeded(self):
        venue, zone, gate = self._setup(venue_capacity=100, zone_capacity=200, gate_flow=200)
        self._admit(zone, gate, 60)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 50)  # 60+50 > 场馆 100
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 60)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])

    def test_reject_when_gate_flow_exceeded(self):
        venue, zone, gate = self._setup(venue_capacity=500, zone_capacity=500, gate_flow=70)
        self._admit(zone, gate, 40)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 40)  # 40+40 > 进场口 70
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 40)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])

    def test_reject_when_zone_full_even_if_other_ledgers_have_room(self):
        venue, zone, gate = self._setup(venue_capacity=500, zone_capacity=100, gate_flow=500)
        self._admit(zone, gate, 100)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 1)  # 区域满了，其它两道还有余量也不放
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 100)

    def test_idempotent_retry_records_batch_once(self):
        venue, zone, gate = self._setup()
        first = self._admit(zone, gate, 30, key="batch-1")
        second = self._admit(zone, gate, 30, key="batch-1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(second["data"]["current_occupancy"], 30)
        records = self._raw(
            "SELECT COUNT(*) FROM admission_records WHERE actor_id = ? AND idem_key = ?",
            ("gate-op", "batch-1"),
        )
        self.assertEqual(records[0][0], 1)

    def test_retry_after_failure_uses_same_key_and_succeeds(self):
        venue, zone, gate = self._setup(venue_capacity=100, zone_capacity=100)
        # 第一笔把场馆额度占到 90
        self._admit(zone, gate, 90, key="fill")
        # 第二笔 20 会因场馆额度不够整笔拒绝（没扣额度、也没写放行记录）
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 20, key="retry-key")
        self.assertIsNone(
            self.service.repository.find_admission_by_idem("gate-op", "retry-key")
        )
        self.assertEqual(self.service.get(zone["id"])["data"]["current_occupancy"], 90)

        # 沿用同一幂等键，改成 10 重试：90+10 正好顶到场馆上限，整笔提交成功
        retried = self._admit(zone, gate, 10, key="retry-key")
        self.assertEqual(retried["data"]["current_occupancy"], 100)
        record = self.service.repository.find_admission_by_idem("gate-op", "retry-key")
        self.assertEqual(record["status"], "committed")
        self.assertEqual(record["count"], 10)
        # 同一批人只记一遍
        total = self._raw(
            "SELECT COUNT(*) FROM admission_records WHERE actor_id = ? AND idem_key = ?",
            ("gate-op", "retry-key"),
        )
        self.assertEqual(total[0][0], 1)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])

    def test_optimistic_lock_retry_then_commit(self):
        venue, zone, gate = self._setup(venue_capacity=200, zone_capacity=200, gate_flow=200)
        original = self.service.repository.commit_admission
        calls = []

        def fake_commit(**kwargs):
            calls.append(1)
            if len(calls) == 1:
                raise ConflictError("quota changed during admission, please retry")
            return original(**kwargs)

        with patch.object(self.service.repository, "commit_admission", side_effect=fake_commit):
            self._admit(zone, gate, 25)
        self.assertEqual(len(calls), 2)
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 25)

    def test_concurrent_admits_later_one_retries_with_latest_remaining(self):
        venue, zone, gate = self._setup(venue_capacity=200, zone_capacity=100, gate_flow=200)
        results = []
        errors = []
        barrier = threading.Barrier(2)

        def admit(count):
            barrier.wait()
            try:
                results.append(self._admit(zone, gate, count))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=admit, args=(60,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # 一笔成功、一笔按最新余量被拒，区域只记 60
        self.assertEqual(len(results), 1)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], ConflictError)
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 60)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])

    def test_concurrent_admits_both_fit_after_retry(self):
        venue, zone, gate = self._setup(venue_capacity=200, zone_capacity=200, gate_flow=200)
        results = []
        errors = []
        barrier = threading.Barrier(2)

        def admit(count):
            barrier.wait()
            try:
                results.append(self._admit(zone, gate, count))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=admit, args=(40,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(len(results), 2)
        self.assertEqual(len(errors), 0)
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 80)
        overview = self.service.quota_overview(venue["id"])
        self.assertTrue(overview["balanced"])

    def test_status_change_recalculates_and_releases_reserved(self):
        venue, zone, gate = self._setup(venue_capacity=100, zone_capacity=100)
        self._admit(zone, gate, 30)
        # 模拟一笔已预留但未放行的额度
        self._raw(
            "UPDATE quota_accounts SET reserved = 20 WHERE scope = 'zone' AND scope_id = ?",
            (zone["id"],),
        )
        # 区域恢复（事件降级后区域恢复）触发重算：预留释放、容量按最新状态回算
        self.service.transition(
            self.coordinator, zone["id"], "evacuate", {"reason": "drill"}
        )
        overview = self.service.quota_overview(venue["id"])
        accounts = {(a["scope"], a["scope_id"]): a for a in overview["accounts"]}
        self.assertEqual(accounts[("zone", zone["id"])]["reserved"], 0)
        self.assertEqual(accounts[("zone", zone["id"])]["capacity"], 0)  # evacuating 不再放人

    def test_legacy_backfill_rebuilds_ledger_from_occupancy(self):
        venue, zone, gate = self._setup(venue_capacity=500, zone_capacity=500, gate_flow=500)
        # 模拟旧数据：直接把区域现有占用写成 120，但没走过额度账
        data = dict(zone["data"])
        data["current_occupancy"] = 120
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute(
                "UPDATE entities SET data = ? WHERE id = ?",
                (json.dumps(data, ensure_ascii=False, sort_keys=True), zone["id"]),
            )
            conn.commit()
        finally:
            conn.close()

        overview = self.service.backfill_quotas()
        self.assertTrue(overview["balanced"])
        accounts = {(a["scope"], a["scope_id"]): a for a in overview["accounts"]}
        self.assertEqual(accounts[("zone", zone["id"])]["released"], 120)
        self.assertEqual(accounts[("venue", venue["id"])]["released"], 120)

    def test_rollback_restores_quota_and_record(self):
        venue, zone, gate = self._setup(venue_capacity=100, zone_capacity=100)
        self._admit(zone, gate, 30, key="rb")
        record = self.service.repository.find_admission_by_idem("gate-op", "rb")
        self.assertEqual(record["status"], "committed")

        rolled = self.service.rollback_admission(record["id"])
        self.assertEqual(rolled["status"], "rolled_back")
        zone = self.service.get(zone["id"])
        self.assertEqual(zone["data"]["current_occupancy"], 0)
        overview = self.service.quota_overview(venue["id"])
        accounts = {(a["scope"], a["scope_id"]): a for a in overview["accounts"]}
        self.assertEqual(accounts[("zone", zone["id"])]["released"], 0)
        self.assertEqual(accounts[("venue", venue["id"])]["released"], 0)
        self.assertEqual(accounts[("gate", gate["id"])]["released"], 0)


if __name__ == "__main__":
    unittest.main()
