package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Query struct {
	Account  string `json:"account_id"`
	SourceID string `json:"source_id,omitempty"`
	ThreadID string `json:"thread_id,omitempty"`
	Sender   string `json:"sender_fbid,omitempty"`
	AfterMS  int64  `json:"after_ms,omitempty"`
	BeforeMS int64  `json:"before_ms,omitempty"`
	Text     string `json:"text,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	Raw      bool   `json:"include_raw,omitempty"`
}
type Page struct {
	Records []Record `json:"records"`
	Next    string   `json:"next_cursor,omitempty"`
}
type position struct {
	Top, Before int64
	QueryHash   string
}

var sourceColumns = map[string][]string{"threads": {"id", "thread_id", "thread_fbid", "thread_key"}, "messages": {"id", "message_id"}, "users": {"id", "pk", "fbid_v2", "interop_messaging_user_fbid"}}

func qualified(table, ref string) (string, string, error) {
	key, value, ok := strings.Cut(ref, ":")
	if !ok || value == "" {
		return "", "", errors.New("source reference must include its original namespace, for example id:123")
	}
	for _, col := range sourceColumns[table] {
		if key == col {
			return col, value, nil
		}
	}
	return "", "", errors.New("unsupported identifier namespace")
}
func (s *Store) ThreadReferences(ctx context.Context, account, ref string) ([]string, error) {
	entity, err := s.ResolveEntity(ctx, account, "threads", ref)
	if err != nil {
		return nil, err
	}
	rows, err := s.queryContext(ctx, `SELECT DISTINCT value FROM source_ids WHERE account_id=? AND thread_entity_id=? AND namespace='thread.fbid'`, account, entity)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}
func (s *Store) Query(ctx context.Context, table string, q Query) (Page, error) {
	out := Page{Records: []Record{}}
	if _, ok := sourceColumns[table]; !ok {
		return out, errors.New("invalid query entity")
	}
	if q.Account == "" {
		return out, errors.New("account_id is required")
	}
	if q.Limit == 0 {
		q.Limit = 30
	}
	if q.Limit < 1 || q.Limit > 100 {
		return out, errors.New("limit must be 1 through 100")
	}
	stable := q
	stable.Cursor = ""
	stable.Raw = false
	b, _ := json.Marshal(stable)
	fingerprint := Hash(b)
	var pos position
	if q.Cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil || json.Unmarshal(b, &pos) != nil || pos.Top <= 0 || pos.Before <= 0 || pos.QueryHash != fingerprint {
			return out, errors.New("invalid cursor for these filters")
		}
	} else {
		if err := s.queryRowContext(ctx, `SELECT coalesce(max(record_id),0) FROM `+table+`_sources WHERE account_id=?`, q.Account).Scan(&pos.Top); err != nil {
			return out, err
		}
		pos.Before = pos.Top + 1
		pos.QueryHash = fingerprint
	}
	where := `account_id=? AND record_id<=?`
	args := []any{q.Account, pos.Top}
	// Choose the latest observation for each original identity within the cursor's
	// high water mark. Earlier raw versions remain retrievable by record reference.

	outer := `record_id<?`
	outerArgs := []any{pos.Before}
	if q.SourceID != "" {
		entity, err := s.ResolveEntity(ctx, q.Account, table, q.SourceID)
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		outer += ` AND entity_id=?`
		outerArgs = append(outerArgs, entity)
	}

	if table == "messages" {
		if q.ThreadID != "" {
			refs, err := s.ThreadReferences(ctx, q.Account, q.ThreadID)
			if err != nil {
				return out, err
			}
			placeholders := strings.TrimSuffix(strings.Repeat("?,", len(refs)), ",")
			outer += ` AND (thread_fbid IN (` + placeholders + `) OR EXISTS (SELECT 1 FROM source_evidence ev JOIN source_observations a ON a.observation_id=ev.observation_id WHERE ev.version_id=ranked.record_id AND a.acquired_thread_id IN (` + placeholders + `)))`
			for range 2 {
				for _, ref := range refs {
					outerArgs = append(outerArgs, ref)
				}
			}
		}
		if q.Sender != "" {
			outer += ` AND sender_fbid=?`
			outerArgs = append(outerArgs, q.Sender)
		}
		if q.AfterMS != 0 {
			outer += ` AND CAST(timestamp_ms AS INTEGER)>=?`
			outerArgs = append(outerArgs, q.AfterMS)
		}
		if q.BeforeMS != 0 {
			outer += ` AND CAST(timestamp_ms AS INTEGER)<?`
			outerArgs = append(outerArgs, q.BeforeMS)
		}
		if q.Text != "" {
			if len(q.Text) > 2000 {
				return out, errors.New("text query too long")
			}
			outer += ` AND (instr(lower(coalesce(text_body,'')),lower(?))>0 OR instr(lower(coalesce(content_text,'')),lower(?))>0 OR instr(lower(coalesce(xma_text,'')),lower(?))>0 OR instr(lower(coalesce(caption_text,'')),lower(?))>0 OR instr(lower(coalesce(igd_snippet,'')),lower(?))>0)`
			// Edits remain source deltas; match their text without manufacturing a
			// replacement message object. Projection returns their source references.
			outer = strings.TrimSuffix(outer, ")") + ` OR EXISTS (SELECT 1 FROM deltas_sources d WHERE d.account_id=ranked.account_id AND (d.message_id=ranked.message_id OR d.message_id=ranked.id OR d.nested_message_id=ranked.id OR d.nested_message_source_id=ranked.message_id) AND (d.thread_fbid=ranked.thread_fbid OR d.thread_fbid=ranked.acquired_thread_id) AND instr(lower(coalesce(d.text_body,'')),lower(?))>0))`
			for range 6 {
				outerArgs = append(outerArgs, q.Text)
			}
		}
	}
	rankOrder := "last_observed_at_ms DESC,record_id DESC"
	if table == "users" {
		rankOrder = "(username IS NOT NULL) DESC,(full_name IS NOT NULL) DESC," + rankOrder
	}
	rankFilter := "rank=1"
	if q.Text != "" {
		rankFilter = "rank>=1"
	}
	sqlText := `WITH ranked AS (SELECT *,row_number() OVER(PARTITION BY source_identity ORDER BY ` + rankOrder + `) AS rank FROM ` + table + `_sources WHERE ` + where + `) SELECT record_id,account_id,source_identity,acquired_thread_id,origin,observed_at_ms,last_observed_at_ms,source_hash,version_id,content_hash FROM ranked WHERE ` + rankFilter + ` AND ` + outer + ` ORDER BY record_id DESC LIMIT ?`
	args = append(args, outerArgs...)
	args = append(args, q.Limit+1)
	rows, err := s.queryContext(ctx, sqlText, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var r Record
		r.Table = table
		if err = rows.Scan(&r.RowID, &r.Account, &r.Identity, &r.Thread, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash); err != nil {
			return out, err
		}
		if len(out.Records) == q.Limit {
			pos.Before = out.Records[len(out.Records)-1].RowID
			b, _ := json.Marshal(pos)
			out.Next = base64.RawURLEncoding.EncodeToString(b)
			break
		}
		out.Records = append(out.Records, r)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	rows.Close()
	size := 0
	for i := range out.Records {
		if err = s.hydrate(ctx, &out.Records[i]); err != nil {
			return out, err
		}
		size += len(out.Records[i].Raw)
		if size > 2<<20 && i+1 < len(out.Records) {
			pos.Before = out.Records[i].RowID
			b, _ := json.Marshal(pos)
			out.Next = base64.RawURLEncoding.EncodeToString(b)
			out.Records = out.Records[:i+1]
			break
		}
	}
	return out, nil
}
func (s *Store) Deltas(ctx context.Context, account string, threads []string, messageIDs []string) ([]Record, bool, error) {
	if len(threads) == 0 {
		return []Record{}, false, nil
	}
	clauses := make([]string, len(threads))
	args := []any{account}
	for i, id := range threads {
		clauses[i] = "?"
		args = append(args, id)
	}
	where := `account_id=? AND thread_fbid IN (` + strings.Join(clauses, ",") + `)`
	if len(messageIDs) > 0 {
		var cond []string
		for _, id := range messageIDs {
			cond = append(cond, `message_id=? OR nested_message_source_id=? OR nested_message_id=?`)
			args = append(args, id, id, id)
		}
		// Thread deletion changes message availability as well.
		where += ` AND ((` + strings.Join(cond, " OR ") + `) OR __typename='SlideUQPPDeleteThread')`
	}
	rows, err := s.queryContext(ctx, `SELECT record_id,source_identity,origin,observed_at_ms,last_observed_at_ms,source_hash,version_id,content_hash FROM deltas_sources WHERE `+where+` ORDER BY CAST(uq_seq_id AS INTEGER),record_id LIMIT 201`, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		r := Record{Account: account, Table: "deltas"}
		if err = rows.Scan(&r.RowID, &r.Identity, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash); err != nil {
			return nil, false, err
		}
		out = append(out, r)
	}
	more := len(out) > 200
	if more {
		out = out[:200]
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}
	rows.Close()
	for i := range out {
		if err = s.hydrate(ctx, &out[i]); err != nil {
			return nil, false, err
		}
	}
	return out, more, nil
}
func SourceRef(r Record) string {
	return fmt.Sprintf("igdm://accounts/%s/records/%s/%s", r.Account, r.Table, strconv.FormatInt(r.RowID, 10))
}

func (s *Store) Record(ctx context.Context, account, table string, id int64) (Record, error) {
	r := Record{Table: table, Account: account}
	if table != "threads" && table != "messages" && table != "users" && table != "deltas" {
		return r, errors.New("invalid source kind")
	}
	err := s.queryRowContext(ctx, `SELECT record_id,source_identity,acquired_thread_id,origin,observed_at_ms,last_observed_at_ms,source_hash,version_id,content_hash FROM `+table+`_sources WHERE account_id=? AND record_id=?`, account, id).Scan(&r.RowID, &r.Identity, &r.Thread, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash)
	if err != nil {
		return r, err
	}
	err = s.hydrate(ctx, &r)
	return r, err
}
