package store

import (
	"context"
	"encoding/json"
)

// ChatRow is a disposable overview of the latest source observations.
type ChatRow struct {
	Identity, Title    string
	Users              json.RawMessage
	Messages, Activity int64
}

func (s *Store) Chats(ctx context.Context, account string) ([]ChatRow, error) {
	var top int64
	if err := s.queryRowContext(ctx, `SELECT coalesce(max(observation_id),0) FROM source_observations WHERE account_id=?`, account).Scan(&top); err != nil {
		return nil, err
	}
	rows, err := s.queryContext(ctx, evidenceCTE+`, tc AS (`+sourceSelection("threads")+`),mc AS (`+sourceSelection("messages")+`),
 t AS (SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY `+rankOrder("threads")+`) AS rank FROM tc),
 m AS (SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY `+rankOrder("messages")+`) AS rank FROM mc),
 counts AS (SELECT mt.thread_entity_id,count(DISTINCT CASE WHEN coalesce(m.reaction_type,'false')!='true' THEN m.entity_id END) AS count,max(CAST(m.timestamp_ms AS INTEGER)) AS stamp FROM m JOIN message_threads mt ON mt.account_id=m.account_id AND mt.message_version_id=m.version_id WHERE m.rank=1 GROUP BY mt.thread_entity_id)
 SELECT t.source_identity,coalesce((SELECT tc.thread_title FROM tc WHERE tc.entity_id=t.entity_id AND json_type(tc.residual,'$.thread_title') IS NOT NULL ORDER BY tc.observation_id DESC,tc.version_id DESC LIMIT 1),''),t.record_id,coalesce(c.count,0),max(coalesce(CAST(t.last_activity_timestamp_ms AS INTEGER),0),coalesce(c.stamp,0)) AS activity FROM t LEFT JOIN counts c ON c.thread_entity_id=t.entity_id WHERE t.rank=1 ORDER BY activity DESC,t.entity_id`, account, top)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ChatRow{}
	var recordIDs []int64
	for rows.Next() {
		var row ChatRow
		var recordID int64
		if err = rows.Scan(&row.Identity, &row.Title, &recordID, &row.Messages, &row.Activity); err != nil {
			return nil, err
		}
		recordIDs = append(recordIDs, recordID)
		out = append(out, row)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i, id := range recordIDs {
		var raw []byte
		var err error
		if s.readTx != nil {
			raw, err = reconstruct(ctx, s.readTx, account, id)
		} else {
			raw, err = reconstruct(ctx, s.DB, account, id)
		}
		if err != nil {
			return nil, err
		}
		var fields map[string]json.RawMessage
		if err = json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
		out[i].Users = fields["users"]
		if len(out[i].Users) == 0 {
			out[i].Users = json.RawMessage(`[]`)
		}
	}
	return out, nil
}

// ReactionLogLinks retains the provider's unqualified message identifiers;
// projection resolves them only when the matching source observation is unique.
func (s *Store) ReactionLogLinks(ctx context.Context, account, thread string) (map[string][]string, error) {
	entity, err := s.ResolveEntity(ctx, account, "threads", thread)
	if err != nil {
		return nil, err
	}
	rows, err := s.queryContext(ctx, `SELECT DISTINCT log.value,target.value FROM deltas_versions d
 JOIN delta_threads dt ON dt.version_id=d.version_id AND dt.thread_entity_id=?
 JOIN source_relations reaction ON reaction.parent_id=d.version_id AND reaction.field='reaction'
 JOIN source_relations logref ON logref.parent_id=reaction.child_id AND logref.field='log_message_id'
 JOIN source_ids lid ON lid.source_id=logref.source_id
 JOIN source_relations targetref ON targetref.parent_id=d.version_id AND targetref.field='message_id'
 JOIN source_ids tid ON tid.source_id=targetref.source_id
 JOIN source_ids log ON log.account_id=d.account_id AND log.message_entity_id=lid.message_entity_id AND log.namespace='message.mid'
 JOIN source_ids target ON target.account_id=d.account_id AND target.message_entity_id=tid.message_entity_id AND target.namespace='message.mid'
 WHERE d.account_id=? AND d.__typename IN ('SlideUQPPCreateReaction','SlideUQPPDeleteReaction')`, entity, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var log, target string
		if err = rows.Scan(&log, &target); err != nil {
			return nil, err
		}
		if log != "" && target != "" {
			out[log] = append(out[log], target)
		}
	}
	return out, rows.Err()
}
