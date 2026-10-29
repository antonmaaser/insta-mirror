package store

import (
	"context"
	"encoding/json"
	"strings"
)

// ChatRow is a disposable overview of the latest source observations.
type ChatRow struct {
	Identity, Title    string
	Users              json.RawMessage
	Messages, Activity int64
}

func (s *Store) Chats(ctx context.Context, account string) ([]ChatRow, error) {
	rows, err := s.queryContext(ctx, `WITH t AS (
 SELECT *,row_number() OVER(PARTITION BY source_identity ORDER BY last_observed_at_ms DESC,record_id DESC) AS rank FROM threads WHERE account_id=?
 ), m AS (
 SELECT *,row_number() OVER(PARTITION BY source_identity ORDER BY last_observed_at_ms DESC,record_id DESC) AS rank FROM messages WHERE account_id=?
 ), refs AS (
 SELECT source_identity,id AS ref FROM threads WHERE account_id=? AND id IS NOT NULL
 UNION SELECT source_identity,thread_fbid FROM threads WHERE account_id=? AND thread_fbid IS NOT NULL
 ), matches AS (
 SELECT DISTINCT refs.source_identity AS chat,m.source_identity AS message,CAST(m.timestamp_ms AS INTEGER) AS stamp,(json_type(m.raw,'$.content.is_reaction_action_log')='true') AS reaction
 FROM m JOIN refs ON (m.thread_fbid=refs.ref OR m.acquired_thread_id=refs.ref) WHERE m.rank=1
 ), counts AS (
 SELECT chat,sum(CASE WHEN reaction=1 THEN 0 ELSE 1 END) AS count,max(stamp) AS stamp FROM matches GROUP BY chat
 ) SELECT t.source_identity,coalesce(json_extract(t.raw,'$.thread_title'),''),coalesce(json_extract(t.raw,'$.users'),'[]'),coalesce(counts.count,0),max(coalesce(CAST(t.last_activity_timestamp_ms AS INTEGER),0),coalesce(counts.stamp,0)) AS activity
 FROM t LEFT JOIN counts ON counts.chat=t.source_identity WHERE t.rank=1 ORDER BY activity DESC,t.source_identity`, account, account, account, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ChatRow{}
	for rows.Next() {
		var row ChatRow
		var users string
		if err = rows.Scan(&row.Identity, &row.Title, &users, &row.Messages, &row.Activity); err != nil {
			return nil, err
		}
		row.Users = []byte(users)
		out = append(out, row)
	}
	return out, rows.Err()
}

// ReactionLogLinks retains the provider's unqualified message identifiers;
// projection resolves them only when the matching source observation is unique.
func (s *Store) ReactionLogLinks(ctx context.Context, account, thread string) (map[string][]string, error) {
	refs, err := s.ThreadReferences(ctx, account, thread)
	if err != nil {
		return nil, err
	}
	args := []any{account}
	for _, ref := range refs {
		args = append(args, ref)
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(refs)), ",")
	rows, err := s.queryContext(ctx, `SELECT DISTINCT json_extract(raw,'$.reaction.log_message_id'),message_id FROM deltas WHERE account_id=? AND thread_fbid IN (`+marks+`) AND __typename IN ('SlideUQPPCreateReaction','SlideUQPPDeleteReaction') AND json_extract(raw,'$.reaction.log_message_id') IS NOT NULL AND message_id IS NOT NULL`, args...)
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
