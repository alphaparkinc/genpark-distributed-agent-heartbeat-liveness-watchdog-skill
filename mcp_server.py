"""MCP Server for Distributed Agent Fleet Heartbeat Liveness Watchdog."""
import sys
import json
import time
from client import DistributedAgentHeartbeatLivenessWatchdog

watchdog = DistributedAgentHeartbeatLivenessWatchdog()

def handle_call_tool(params):
    name = params.get("name")
    args = params.get("arguments", {})
    if name != "monitor_agent_fleet_liveness":
        raise ValueError(f"Unknown tool: {name}")

    action = args.get("action", "audit_fleet_health")
    agent_id = args.get("agent_id", "worker_node_1")

    if action == "record_heartbeat":
        return watchdog.record_heartbeat(
            agent_id=agent_id,
            reported_status=args.get("reported_status", "HEALTHY"),
            cpu_load_pct=float(args.get("cpu_load_pct", 20.0)),
            active_task_id=args.get("active_task_id")
        )
    elif action == "audit_fleet_health":
        return watchdog.audit_fleet_health()
    elif action == "trigger_worker_failover":
        return watchdog.trigger_worker_failover(
            dead_agent_id=agent_id,
            backup_agent_id=args.get("backup_agent_id", "backup_worker_node")
        )
    else:
        raise ValueError(f"Invalid action: {action}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Running self-test...")
        watchdog.record_heartbeat("worker_alpha", "HEALTHY", 10.0, "TASK-101")
        watchdog.record_heartbeat("worker_beta", "HEALTHY", 12.0, "TASK-102")
        audit = watchdog.audit_fleet_health()
        assert audit["total_workers"] == 2
        assert audit["healthy_workers_count"] == 2

        # Simulate timeout on alpha
        watchdog.workers["worker_alpha"]["last_heartbeat"] = time.time() - 100.0
        audit_zombie = watchdog.audit_fleet_health()
        assert audit_zombie["zombie_workers_count"] == 1
        fo = watchdog.trigger_worker_failover("worker_alpha", "worker_beta")
        assert fo["status"] == "FAILOVER_MIGRATED_SUCCESSFULLY"
        print("Self-test PASSED!")
        sys.exit(0)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            msg_id = req.get("id")
            method = req.get("method")
            if method == "initialize":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "serverInfo": {"name": "DistributedAgentHeartbeatLivenessWatchdog", "version": "1.0.0"},
                        "capabilities": {"tools": {}}
                    }
                }
            elif method == "tools/list":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "tools": [{
                            "name": "monitor_agent_fleet_liveness",
                            "description": "Record agent heartbeats, calculate dynamic jitter-aware timeout thresholds, detect dead or hung worker nodes, and trigger failover reassignments.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "action": {"type": "string", "enum": ["record_heartbeat", "audit_fleet_health", "trigger_worker_failover"]},
                                    "agent_id": {"type": "string"}
                                },
                                "required": ["action"]
                            }
                        }]
                    }
                }
            elif method == "tools/call":
                res = handle_call_tool(req.get("params", {}))
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(res, indent=2)}]}
                }
            else:
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
            print(json.dumps(resp), flush=True)
        except Exception as e:
            err_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": str(e)}}
            print(json.dumps(err_resp), flush=True)

if __name__ == "__main__":
    main()
