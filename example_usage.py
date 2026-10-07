"""Example usage for DistributedAgentHeartbeatLivenessWatchdog."""
import sys
import json
import time
from client import DistributedAgentHeartbeatLivenessWatchdog

sys.stdout.reconfigure(encoding='utf-8')

def main():
    print("=== Distributed Agent Fleet Liveness & Failover Watchdog Demo ===")
    watchdog = DistributedAgentHeartbeatLivenessWatchdog(baseline_timeout_seconds=5.0)

    # 1. Register fleet workers with regular heartbeat pulses
    print("\n--- 1. Ingesting Agent Heartbeat Pulses ---")
    watchdog.record_heartbeat("workbuddy_worker_01", "HEALTHY", cpu_load_pct=22.5, active_task_id="TASK-EXPORT-PPT")
    watchdog.record_heartbeat("workbuddy_worker_02", "HEALTHY", cpu_load_pct=34.1, active_task_id="TASK-AUDIT-MERKLE")
    print(f"Registered {len(watchdog.workers)} active fleet workers.")

    # 2. Audit fleet health (all operational)
    print("\n--- 2. Auditing Fleet Health Status ---")
    health = watchdog.audit_fleet_health()
    print(f"Fleet Status: {health['fleet_status']} (Healthy: {health['healthy_workers_count']}/{health['total_workers']})")

    # 3. Simulate sudden unresponsiveness on worker 01
    print("\n--- 3. Simulating Node Timeout / Zombie Worker ---")
    watchdog.workers["workbuddy_worker_01"]["last_heartbeat"] = time.time() - 15.0
    degraded_health = watchdog.audit_fleet_health()
    print(f"Fleet Status: {degraded_health['fleet_status']} | Stalled Workers: {degraded_health['zombie_workers_count']}")
    for z in degraded_health["zombie_workers"]:
        print(f"  [Zombie Alert]: Node @{z['agent_id']} stalled for {z['elapsed_since_heartbeat_seconds']}s (Task: {z['stalled_task_id']})")

    # 4. Trigger automated failover task migration
    print("\n--- 4. Executing Failover Lease Migration ---")
    failover = watchdog.trigger_worker_failover("workbuddy_worker_01", backup_agent_id="workbuddy_worker_02")
    print(f"Failover Token: {failover['failover_token']}")
    print(f"Migrated Task '{failover['migrated_task_id']}' to Worker @{failover['assigned_backup_agent_id']}")

if __name__ == "__main__":
    main()
