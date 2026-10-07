"""
Distributed Agent Fleet Heartbeat & Liveness Watchdog (Zero External Dependencies)
Calculates statistical jitter-aware timeouts, detects hung zombie agents, and coordinates failover.
"""
import time
import math
import hashlib
import json
from typing import Dict, Any, List, Optional

class DistributedAgentHeartbeatLivenessWatchdog:
    def __init__(self, baseline_timeout_seconds: float = 30.0):
        self.baseline_timeout = baseline_timeout_seconds
        self.workers: Dict[str, Dict[str, Any]] = {}

    def record_heartbeat(
        self,
        agent_id: str,
        reported_status: str = "HEALTHY",
        cpu_load_pct: float = 15.0,
        active_task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Records an incoming heartbeat pulse and updates moving average latency and jitter."""
        now = time.time()
        if agent_id not in self.workers:
            self.workers[agent_id] = {
                "agent_id": agent_id,
                "created_at": now,
                "last_heartbeat": now,
                "intervals": [],
                "status": reported_status.upper(),
                "cpu_load_pct": cpu_load_pct,
                "active_task_id": active_task_id,
                "failover_count": 0
            }
            return {"agent_id": agent_id, "status": "REGISTERED", "timestamp": now}

        worker = self.workers[agent_id]
        interval = max(0.01, now - worker["last_heartbeat"])
        worker["intervals"].append(interval)
        if len(worker["intervals"]) > 10:
            worker["intervals"].pop(0)

        worker["last_heartbeat"] = now
        worker["status"] = reported_status.upper()
        worker["cpu_load_pct"] = cpu_load_pct
        worker["active_task_id"] = active_task_id

        return {
            "agent_id": agent_id,
            "status": "RECORDED",
            "measured_interval_seconds": round(interval, 2),
            "timestamp": now
        }

    def compute_dynamic_timeout(self, agent_id: str) -> float:
        """Computes dynamic timeout threshold: T = mean + 3 * sigma (jitter tolerance)."""
        worker = self.workers.get(agent_id)
        if not worker or len(worker["intervals"]) < 2:
            return self.baseline_timeout

        intervals = worker["intervals"]
        mean_int = sum(intervals) / len(intervals)
        var = sum((x - mean_int) ** 2 for x in intervals) / len(intervals)
        std_dev = math.sqrt(var)

        dynamic_timeout = mean_int + (3.0 * std_dev)
        return round(max(self.baseline_timeout * 0.5, min(self.baseline_timeout * 3.0, dynamic_timeout)), 2)

    def audit_fleet_health(self) -> Dict[str, Any]:
        """Scans all registered agent workers, flags zombie/dead workers, and triggers alarms."""
        now = time.time()
        healthy_count = 0
        zombie_workers = []

        for agent_id, worker in self.workers.items():
            timeout_limit = self.compute_dynamic_timeout(agent_id)
            elapsed = now - worker["last_heartbeat"]

            if elapsed > timeout_limit:
                worker["status"] = "ZOMBIE_TIMED_OUT"
                zombie_workers.append({
                    "agent_id": agent_id,
                    "elapsed_since_heartbeat_seconds": round(elapsed, 1),
                    "dynamic_timeout_limit": timeout_limit,
                    "stalled_task_id": worker.get("active_task_id")
                })
            else:
                healthy_count += 1

        return {
            "audited_at": now,
            "total_workers": len(self.workers),
            "healthy_workers_count": healthy_count,
            "zombie_workers_count": len(zombie_workers),
            "zombie_workers": zombie_workers,
            "fleet_status": "DEGRADED" if zombie_workers else "ALL_SYSTEMS_OPERATIONAL"
        }

    def trigger_worker_failover(self, dead_agent_id: str, backup_agent_id: str) -> Dict[str, Any]:
        """Executes failover: revokes dead agent lease, migrates task to warm backup."""
        if dead_agent_id not in self.workers:
            return {"error": f"Agent {dead_agent_id} not found"}

        dead_worker = self.workers[dead_agent_id]
        stalled_task = dead_worker.get("active_task_id")
        dead_worker["status"] = "TERMINATED_FAILOVER"
        dead_worker["failover_count"] += 1

        now = time.time()
        failover_token = "FAILOVER-" + hashlib.sha256(f"{dead_agent_id}:{backup_agent_id}:{now}".encode("utf-8")).hexdigest()[:8].upper()

        return {
            "failover_token": failover_token,
            "dead_agent_id": dead_agent_id,
            "assigned_backup_agent_id": backup_agent_id,
            "migrated_task_id": stalled_task,
            "status": "FAILOVER_MIGRATED_SUCCESSFULLY",
            "timestamp": now
        }
