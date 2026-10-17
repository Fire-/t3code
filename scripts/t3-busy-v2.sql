-- Busy probe for orchestration v2 (userdata/statev2.sqlite), shared by
-- scripts/daemon.sh and `mise run daemon-status`. Prints nothing when idle,
-- otherwise "reason:threadCount,...".
--
-- Mirrors ProjectionStore.getRecoveryThreadIds("runtime") minus two terms:
-- provider sessions (`ready` is the normal idle state, so counting them pins
-- the daemon forever) and pending outbox rows (the outbox is durable and
-- resumes after a restart).
WITH busy(thread_id, reason) AS (
  SELECT thread_id, 'run' FROM orchestration_v2_projection_runs
   WHERE status IN ('preparing','starting','running','waiting')
  UNION ALL
  SELECT thread_id, 'queued' FROM orchestration_v2_projection_runs
   WHERE status = 'queued'
     AND CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.queueHeld') IS NOT 1 ELSE 1 END
  UNION ALL
  SELECT thread_id, 'request' FROM orchestration_v2_projection_runtime_requests
   WHERE status = 'pending'
  UNION ALL
  SELECT COALESCE(pt.thread_id, n.thread_id), 'background'
    FROM orchestration_v2_projection_provider_threads pt
    LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id = pt.owner_node_id
   WHERE pt.status = 'active'
      OR CASE WHEN json_valid(pt.payload_json)
              THEN json_array_length(pt.payload_json, '$.pendingBackgroundTasks') > 0 ELSE 0 END
  UNION ALL
  SELECT thread_id, 'subagent' FROM orchestration_v2_projection_subagents
   WHERE origin = 'provider_native' AND status IN ('pending','running','waiting')
  UNION ALL
  SELECT i.thread_id, 'work-item' FROM orchestration_v2_projection_turn_items i
    LEFT JOIN orchestration_v2_projection_runs r ON r.run_id = i.run_id
   WHERE i.type IN ('command_execution','dynamic_tool','subagent')
     AND i.status IN ('pending','running','waiting')
     AND (i.run_id IS NULL OR r.status IS NOT 'rolled_back')
  UNION ALL
  SELECT thread_id, 'effect' FROM orchestration_v2_effect_outbox WHERE status = 'running'
)
SELECT group_concat(reason || ':' || n, ',')
FROM (
  SELECT b.reason, COUNT(DISTINCT b.thread_id) AS n
  FROM busy b
  JOIN orchestration_v2_projection_threads t
    ON t.thread_id = b.thread_id AND t.deleted_at IS NULL
  GROUP BY b.reason
)
HAVING COUNT(*) > 0;
