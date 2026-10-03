import threading
import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition
from src.repository import SQLiteRepository
from src.rules import QUOTA_UNLIMITED, RuleEngine
from src.service import DomainService


class QuotaTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repository = SQLiteRepository(Path(self.tmp.name) / "quota.db")
        self.service = DomainService(self.repository, RuleEngine())
        self.coordinator = Actor("coordinator", "coordinator")
        self.supervisor = Actor("supervisor", "supervisor")
        self.operator = Actor("operator", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _setup(self, capacity=100, total_capacity=None, zones=1):
        venue_data = {"name": "V", "address": "A"}
        if total_capacity is not None:
            venue_data["total_capacity"] = total_capacity
        venue = self.service.create(self.coordinator, "venue", venue_data)
        result = []
        for index in range(zones):
            zone = self.service.create(
                self.coordinator,
                "zone",
                {"venue_id": venue["id"], "name": "Z%s" % index, "capacity": capacity},
            )
            gate = self.service.create(
                self.coordinator,
                "gate",
                {"venue_id": venue["id"], "name": "G%s" % index, "zone_ids": [zone["id"]]},
            )
            self.service.transition(self.operator, gate["id"], "open", {"operator_id": "op"})
            self.service.transition(self.operator, zone["id"], "open", {"checklist": "clear"})
            result.append((zone, gate))
        return venue, result

    def _account(self, scope, ref_id):
        return self.repository.get_account("%s:%s" % (scope, ref_id))

    def _admit(self, zone, gate, count, key=None, **extra):
        data = {"gate_id": gate["id"], "count": count, "admitted_at": "t"}
        data.update(extra)
        if key:
            data["idempotency_key"] = key
        return self.service.transition(self.operator, zone["id"], "admit", data)

    def test_admit_deducts_three_quotas_atomically(self):
        venue, [(zone, gate)] = self._setup(capacity=100, total_capacity=500)
        result = self._admit(zone, gate, 30, key="batch-1")
        self.assertEqual(result["data"]["current_occupancy"], 30)
        self.assertFalse(result["admission"]["replayed"])
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 30)
        self.assertEqual(self._account("venue", venue["id"])["used_amount"], 30)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 30)
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 100)
        self.assertEqual(self._account("venue", venue["id"])["limit_amount"], 500)
        self.assertEqual(self._account("gate", gate["id"])["limit_amount"], QUOTA_UNLIMITED)
        entries = self.service.quota_ledger(batch_id=result["admission"]["batch_id"])
        self.assertEqual(len(entries), 3)
        self.assertEqual({entry["delta_used"] for entry in entries}, {30})
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_any_insufficient_quota_rejects_whole_batch(self):
        venue, [(zone, gate)] = self._setup(capacity=100, total_capacity=45)
        # 区域余量够、场馆总容量不够：整笔拒绝，一道都不扣。
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 50)
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 0)
        self.assertEqual(self._account("venue", venue["id"])["used_amount"], 0)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 0)
        self.assertEqual(self.service.get(zone["id"])["data"]["current_occupancy"], 0)
        admits = [e for e in self.service.quota_ledger() if e["reason"] == "admit"]
        self.assertEqual(admits, [])
        # 进场口放行上限不够：同样整笔拒绝。
        self.service.transition(
            self.supervisor, gate["id"], "restrict", {"reason": "crowd", "flow_limit": 10}
        )
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 11)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 0)
        self._admit(zone, gate, 10)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 10)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 1)
        # 恢复放行上限后可以再放行。
        self.service.transition(self.operator, gate["id"], "restore", {"reason": "ok"})
        self._admit(zone, gate, 5)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 15)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_idempotent_retry_counts_batch_once(self):
        venue, [(zone, gate)] = self._setup()
        first = self._admit(zone, gate, 30, key="same-key")
        second = self._admit(zone, gate, 30, key="same-key")
        self.assertTrue(second["admission"]["replayed"])
        self.assertEqual(first["admission"]["batch_id"], second["admission"]["batch_id"])
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 30)
        self.assertEqual(self.service.get(zone["id"])["data"]["current_occupancy"], 30)
        self._admit(zone, gate, 30)
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 60)

    def test_failed_admission_rolls_back_and_retry_reuses_key(self):
        venue, [(zone, gate)] = self._setup()
        self._admit(zone, gate, 20, key="seed")
        # 版本不符导致中途失败：扣掉的额度和放行记录一起退回。
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.operator,
                zone["id"],
                "admit",
                {"gate_id": gate["id"], "count": 10, "admitted_at": "t", "idempotency_key": "retry-1"},
                expected_version=999,
            )
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 20)
        self.assertEqual(self._account("venue", venue["id"])["used_amount"], 20)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 20)
        self.assertEqual(self.service.get(zone["id"])["data"]["current_occupancy"], 20)
        self.assertIsNone(self.repository.get_batch("operator:retry-1"))
        self.assertEqual(self.service.quota_ledger(batch_id="operator:retry-1"), [])
        # 沿用同一个幂等键重试：只记一遍。
        self._admit(zone, gate, 10, key="retry-1")
        again = self._admit(zone, gate, 10, key="retry-1")
        self.assertTrue(again["admission"]["replayed"])
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 30)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_concurrent_admissions_apply_against_latest_remaining(self):
        venue, [(zone, gate)] = self._setup(capacity=100)
        barrier = threading.Barrier(2)
        outcomes = []

        def submit(count):
            barrier.wait()
            try:
                self._admit(zone, gate, count)
                outcomes.append("ok")
            except ConflictError:
                outcomes.append("rejected")

        threads = [threading.Thread(target=submit, args=(60,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # 区域只放得下一笔：后到的按最新余量重来后被整笔拒绝。
        self.assertEqual(sorted(outcomes), ["ok", "rejected"])
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 60)
        self.assertEqual(self.service.get(zone["id"])["data"]["current_occupancy"], 60)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_concurrent_admissions_both_fit(self):
        venue, [(zone, gate)] = self._setup(capacity=200)
        barrier = threading.Barrier(2)
        outcomes = []

        def submit(count):
            barrier.wait()
            self._admit(zone, gate, count)
            outcomes.append("ok")

        threads = [threading.Thread(target=submit, args=(60,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(outcomes, ["ok", "ok"])
        self.assertEqual(self._account("zone", zone["id"])["used_amount"], 120)
        self.assertEqual(self._account("venue", venue["id"])["used_amount"], 120)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_zone_status_change_recalculates_reserved_quota(self):
        venue, [(zone, gate)] = self._setup(capacity=100)
        self._admit(zone, gate, 30)
        self.service.transition(
            self.supervisor, zone["id"], "restrict", {"reason": "crowd", "admit_limit": 40}
        )
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 40)
        recalc = self.service.quota_ledger(account_id="zone:" + zone["id"])[-1]
        self.assertEqual(recalc["reason"], "recalc")
        self.assertEqual(recalc["delta_limit"], -60)
        self.assertEqual(recalc["detail"]["released"], 60)
        # limited状态不因放行而解除，剩余额度继续受控。
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 11)
        self._admit(zone, gate, 10)
        self.assertEqual(self.service.get(zone["id"])["status"], "limited")
        self.service.transition(self.supervisor, zone["id"], "evacuate", {"reason": "fire"})
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 0)
        with self.assertRaises(InvalidTransition):
            self._admit(zone, gate, 1)
        # 区域恢复后按最新状态回算。
        self.service.transition(self.supervisor, zone["id"], "recover", {"checklist": "done"})
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 100)
        self._admit(zone, gate, 60)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_incident_hold_and_downgrade_recalculate(self):
        venue, [(zone, gate)] = self._setup(capacity=1000)
        self._admit(zone, gate, 400)
        incident = self.service.create(
            self.operator,
            "incident",
            {
                "venue_id": venue["id"],
                "zone_id": zone["id"],
                "source_ref": "cam-1",
                "incident_type": "crowd",
                "severity": "high",
                "reported_at": "t1",
            },
        )
        # high事件预留10%处置缓冲：上限1000 -> 900。
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 900)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 501)
        self._admit(zone, gate, 500)
        # 事件降级：按最新严重度回算预留。
        incident = self.service.transition(
            self.supervisor, incident["id"], "correct", {"reason": "downgrade", "severity": "low"}
        )
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 1000)
        with self.assertRaises(ConflictError):
            self._admit(zone, gate, 101)
        # 事件升级再结案：预留先加后放。
        self.service.transition(
            self.supervisor, incident["id"], "correct", {"reason": "upgrade", "severity": "critical"}
        )
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 800)
        self.service.transition(self.coordinator, incident["id"], "dispatch", {"commander_id": "c1"})
        self.service.transition(self.coordinator, incident["id"], "resolve", {"resolution": "cleared"})
        self.assertEqual(self._account("zone", zone["id"])["limit_amount"], 1000)
        self._admit(zone, gate, 100)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_venue_limit_close_reopen_recalculate(self):
        venue, pairs = self._setup(capacity=100, zones=2)
        (zone1, gate1), (zone2, gate2) = pairs
        self.assertEqual(self._account("venue", venue["id"])["limit_amount"], 200)
        self.service.transition(
            self.coordinator, venue["id"], "limit", {"reason": "crowd", "capacity_limit": 120}
        )
        self.assertEqual(self._account("venue", venue["id"])["limit_amount"], 120)
        self._admit(zone1, gate1, 100)
        with self.assertRaises(ConflictError):
            self._admit(zone2, gate2, 30)
        self._admit(zone2, gate2, 20)
        self.service.transition(self.coordinator, venue["id"], "close", {"reason": "night"})
        self.assertEqual(self._account("venue", venue["id"])["limit_amount"], 0)
        self.service.transition(self.coordinator, venue["id"], "reopen", {"reason": "day"})
        self.assertEqual(self._account("venue", venue["id"])["limit_amount"], 200)
        self.assertEqual(self.service.verify_quota_ledger(), [])

    def test_gate_reopen_resets_release_window(self):
        venue, [(zone, gate)] = self._setup(capacity=100)
        self._admit(zone, gate, 40)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 40)
        self.service.transition(self.supervisor, gate["id"], "close", {"reason": "night"})
        self.assertEqual(self._account("gate", gate["id"])["limit_amount"], 0)
        self.service.transition(self.operator, gate["id"], "open", {"operator_id": "op"})
        account = self._account("gate", gate["id"])
        self.assertEqual(account["used_amount"], 0)
        self.assertEqual(account["limit_amount"], QUOTA_UNLIMITED)
        reasons = [entry["reason"] for entry in self.service.quota_ledger(account_id="gate:" + gate["id"])]
        self.assertIn("reset", reasons)
        self._admit(zone, gate, 10)
        self.assertEqual(self._account("gate", gate["id"])["used_amount"], 10)

    def test_migration_backfills_from_existing_occupancy(self):
        # 旧数据：直接写库，不经过额度账。
        venue = self.repository.create_entity(
            "venue-old", "venue", "ready", {"name": "V", "address": "A"}, "tester"
        )
        zone = self.repository.create_entity(
            "zone-old", "zone", "open",
            {"venue_id": "venue-old", "name": "Z", "capacity": 100, "current_occupancy": 40},
            "tester",
        )
        self.repository.create_entity(
            "gate-old", "gate", "open",
            {"venue_id": "venue-old", "name": "G", "zone_ids": ["zone-old"]},
            "tester",
        )
        self.assertEqual(
            sorted(self.service.verify_quota_ledger()),
            ["missing venue account: venue-old", "missing zone account: zone-old"],
        )
        result = self.service.migrate_quotas()
        self.assertEqual(
            sorted(result["created"]), ["gate:gate-old", "venue:venue-old", "zone:zone-old"]
        )
        self.assertEqual(self._account("zone", "zone-old")["used_amount"], 40)
        self.assertEqual(self._account("zone", "zone-old")["limit_amount"], 100)
        self.assertEqual(self._account("venue", "venue-old")["used_amount"], 40)
        self.assertEqual(self._account("gate", "gate-old")["used_amount"], 0)
        self.assertEqual(self.service.verify_quota_ledger(), [])
        # 迁移可重复执行，不会重复回填。
        self.assertEqual(self.service.migrate_quotas()["created"], [])
        # 回填后放行接着旧占用继续记。
        self.service.transition(
            self.operator,
            "zone-old",
            "admit",
            {"gate_id": "gate-old", "count": 10, "admitted_at": "t"},
        )
        self.assertEqual(self._account("zone", "zone-old")["used_amount"], 50)
        self.assertEqual(self._account("venue", "venue-old")["used_amount"], 50)
        self.assertEqual(self.service.verify_quota_ledger(), [])


if __name__ == "__main__":
    unittest.main()
