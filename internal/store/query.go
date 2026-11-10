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
	EntityID int64  `json:"entity_id,omitempty"`
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
	Top, Before, IdentityRevision int64
	QueryHash                     string
}

var sourceColumns = map[string][]string{"threads": {"id", "thread_id", "thread_fbid", "thread_key"}, "messages": {"id", "message_id"}, "users": {"id", "pk", "fbid_v2", "interop_messaging_user_fbid"}}

func qualified(kind, ref string) (string, string, error) {
	key, value, ok := strings.Cut(ref, ":")
	if !ok || value == "" {
		return "", "", errors.New("source reference requires a namespace, for example id:123")
	}
	for _, col := range sourceColumns[kind] {
		if col == key {
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
	return s.entityIDs(ctx, account, "threads", entity, "thread.fbid")
}
func (s *Store) entityIDs(ctx context.Context, account, kind string, entity int64, namespace string) ([]string, error) {
	rows, err := s.queryContext(ctx, `SELECT DISTINCT value FROM source_ids WHERE account_id=? AND `+ownerColumn(kind)+`=? AND namespace=? ORDER BY value`, account, entity, namespace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

// Only root acquisitions are stored. The recursive read derives provenance of
// embedded versions, bounded by the cursor's immutable observation watermark.
const evidenceCTE = `WITH RECURSIVE evidence(account_id,version_id,observation_id) AS (
 SELECT account_id,version_id,observation_id FROM source_observations WHERE account_id=? AND observation_id<=?
 UNION SELECT r.account_id,r.child_id,e.observation_id FROM evidence e JOIN source_relations r ON r.account_id=e.account_id AND r.parent_id=e.version_id WHERE r.child_id IS NOT NULL
 ), visible AS (
 SELECT e.account_id,e.version_id,max(e.observation_id) AS observation_id,min(o.observed_at_ms) AS observed_at_ms,max(o.last_observed_at_ms) AS last_observed_at_ms
 FROM evidence e JOIN source_observations o ON o.observation_id=e.observation_id GROUP BY e.account_id,e.version_id
 )`

func sourceSelection(kind string) string {
	return `SELECT v.*,p.origin,p.acquired_thread_id,p.source_hash,e.observation_id,e.observed_at_ms,e.last_observed_at_ms FROM ` + kind + `_versions v JOIN visible e ON e.account_id=v.account_id AND e.version_id=v.version_id JOIN source_observations p ON p.observation_id=e.observation_id WHERE v.entity_id>0`
}
func sourceCTE(kind string) string {
	return evidenceCTE + `, candidates AS (` + sourceSelection(kind) + `)`
}
func rankOrder(kind string) string {
	switch kind {
	case "messages":
		return `(json_type(residual,'$.message_id') IS NOT NULL OR origin IN ('get_slide_thread_nullable','fetch__SlideThread','dgw')) DESC,observation_id DESC,version_id DESC`
	case "threads":
		return `(json_type(residual,'$.thread_subtype') IS NOT NULL) DESC,observation_id DESC,version_id DESC`
	default:
		return `observation_id DESC,version_id DESC`
	}
}

const recordColumns = `record_id,account_id,source_identity,acquired_thread_id,origin,observed_at_ms,last_observed_at_ms,source_hash,version_id,content_hash,entity_id,observation_id`

func scanRecord(row interface{ Scan(...any) error }, r *Record) error {
	return row.Scan(&r.RowID, &r.Account, &r.Identity, &r.Thread, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash, &r.Entity, &r.Observation)
}

func (s *Store) Query(ctx context.Context, kind string, q Query) (Page, error) {
	out := Page{Records: []Record{}}
	if ownerColumn(kind) == "" {
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
	if len(q.Text) > 2000 {
		return out, errors.New("text query too long")
	}
	stable := q
	stable.Cursor = ""
	stable.Raw = false
	b, _ := json.Marshal(stable)
	fingerprint := Hash(b)
	var identity State
	if err := s.State(ctx, q.Account, "identity", "revision", &identity); err != nil {
		return out, err
	}
	var pos position
	if q.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil || json.Unmarshal(raw, &pos) != nil || pos.Top <= 0 || pos.Before <= 0 || pos.QueryHash != fingerprint {
			return out, errors.New("invalid cursor for these filters")
		}
		if pos.IdentityRevision != identity.Revision {
			return out, errors.New("identity mappings changed; restart pagination")
		}
	} else {
		if err := s.queryRowContext(ctx, `SELECT coalesce(max(observation_id),0) FROM source_observations WHERE account_id=?`, q.Account).Scan(&pos.Top); err != nil {
			return out, err
		}
		pos.Before = 1<<63 - 1
		pos.QueryHash = fingerprint
		pos.IdentityRevision = identity.Revision
	}
	if pos.Top == 0 {
		return out, nil
	}
	where := `rank=1 AND entity_id<?`
	args := []any{q.Account, pos.Top, pos.Before}
	if q.SourceID != "" || q.EntityID != 0 {
		ref := q.SourceID
		if q.EntityID != 0 {
			if ref != "" {
				return out, errors.New("use either source_id or entity_id")
			}
			ref = fmt.Sprintf("entity:%d", q.EntityID)
		}
		entity, err := s.ResolveEntity(ctx, q.Account, kind, ref)
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		where += ` AND entity_id=?`
		args = append(args, entity)
	}
	if kind == "messages" {
		if q.ThreadID != "" {
			entity, err := s.ResolveEntity(ctx, q.Account, "threads", q.ThreadID)
			if errors.Is(err, sql.ErrNoRows) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			where += ` AND EXISTS(SELECT 1 FROM message_threads mt WHERE mt.account_id=ranked.account_id AND mt.message_version_id=ranked.version_id AND mt.thread_entity_id=?)`
			args = append(args, entity)
		}
		if q.Sender != "" {
			entity, err := s.ResolveEntity(ctx, q.Account, "users", "interop_messaging_user_fbid:"+q.Sender)
			if errors.Is(err, sql.ErrNoRows) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			where += ` AND EXISTS(SELECT 1 FROM message_senders ms WHERE ms.account_id=ranked.account_id AND ms.message_version_id=ranked.version_id AND ms.user_entity_id=?)`
			args = append(args, entity)
		}
		if q.AfterMS != 0 {
			where += ` AND CAST(timestamp_ms AS INTEGER)>=?`
			args = append(args, q.AfterMS)
		}
		if q.BeforeMS != 0 {
			where += ` AND CAST(timestamp_ms AS INTEGER)<?`
			args = append(args, q.BeforeMS)
		}
	}
	cte := sourceCTE(kind) + `, ranked AS (SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY ` + rankOrder(kind) + `) AS rank FROM candidates)`
	matchColumn := `'[]'`
	if q.Text != "" && kind == "messages" {
		// Search all acquired versions and edit deltas, but select one current item
		// per identity. Exact matching source versions are returned as evidence.
		cte += `, matches AS (
  SELECT DISTINCT c.entity_id,c.version_id,'messages' AS matched_kind FROM candidates c WHERE instr(lower(coalesce(c.text_body,'')||char(10)||coalesce(c.content_text,'')||char(10)||coalesce(c.xma_text,'')||char(10)||coalesce(c.caption_text,'')||char(10)||coalesce(c.igd_snippet,'')),lower(?))>0
  UNION SELECT dm.message_entity_id,d.version_id,'deltas' FROM deltas_versions d JOIN visible ev ON ev.version_id=d.version_id JOIN delta_messages dm ON dm.account_id=d.account_id AND dm.version_id=d.version_id WHERE instr(lower(coalesce(d.text_body,'')),lower(?))>0
  )`
		// CTE placeholders precede the outer filter placeholders.
		args = append([]any{q.Account, pos.Top, q.Text, q.Text}, args[2:]...)
		where += ` AND EXISTS(SELECT 1 FROM matches mm WHERE mm.entity_id=ranked.entity_id)`
		matchColumn = `(SELECT json_group_array(json_object('id',version_id,'kind',matched_kind)) FROM matches mm WHERE mm.entity_id=ranked.entity_id)`
	}
	rows, err := s.queryContext(ctx, cte+` SELECT `+recordColumns+`,`+matchColumn+` FROM ranked WHERE `+where+` ORDER BY entity_id DESC LIMIT ?`, append(args, q.Limit+1)...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		r := Record{Table: kind, watermark: pos.Top}
		var matches string
		if err = rows.Scan(&r.RowID, &r.Account, &r.Identity, &r.Thread, &r.Origin, &r.Observed, &r.LastObserved, &r.Hash, &r.Version, &r.ContentHash, &r.Entity, &r.Observation, &matches); err != nil {
			rows.Close()
			return out, err
		}
		if len(out.Records) == q.Limit {
			out.Next = encodePosition(pos, out.Records[len(out.Records)-1].Entity)
			break
		}
		var ids []struct {
			ID   int64  `json:"id"`
			Kind string `json:"kind"`
		}
		if err = json.Unmarshal([]byte(matches), &ids); err != nil {
			rows.Close()
			return out, err
		}
		for _, id := range ids {
			r.Matches = append(r.Matches, fmt.Sprintf("igdm://accounts/%s/records/%s/%d", q.Account, id.Kind, id.ID))
		}
		out.Records = append(out.Records, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	size := 0
	for i := range out.Records {
		if err = s.hydrate(ctx, &out.Records[i]); err != nil {
			return out, err
		}
		size += len(out.Records[i].Raw)
		if size > 2<<20 && i+1 < len(out.Records) {
			out.Next = encodePosition(pos, out.Records[i].Entity)
			out.Records = out.Records[:i+1]
			break
		}
	}
	return out, nil
}
func encodePosition(pos position, before int64) string {
	pos.Before = before
	b, _ := json.Marshal(pos)
	return base64.RawURLEncoding.EncodeToString(b)
}
func SourceRef(r Record) string {
	return fmt.Sprintf("igdm://accounts/%s/records/%s/%s", r.Account, r.Table, strconv.FormatInt(r.RowID, 10))
}
func (s *Store) Record(ctx context.Context, account, kind string, id int64) (Record, error) {
	r := Record{Table: kind, Account: account}
	if ownerColumn(kind) == "" && kind != "deltas" {
		err := s.queryRowContext(ctx, `SELECT v.version_id,p.origin,p.acquired_thread_id,p.source_hash,v.content_hash,e.observation_id,e.observed_at_ms,e.last_observed_at_ms FROM source_versions v JOIN source_visible e ON e.account_id=v.account_id AND e.version_id=v.version_id JOIN source_observations p ON p.observation_id=e.observation_id WHERE v.account_id=? AND v.kind=? AND v.version_id=?`, account, kind, id).Scan(&r.Version, &r.Origin, &r.Thread, &r.Hash, &r.ContentHash, &r.Observation, &r.Observed, &r.LastObserved)
		if err != nil {
			return r, err
		}
		r.RowID = r.Version
		r.Identity = fmt.Sprintf("version:%d", r.Version)
		err = s.hydrate(ctx, &r)
		return r, err
	}
	err := scanRecord(s.queryRowContext(ctx, `SELECT `+recordColumns+` FROM `+kind+`_sources WHERE account_id=? AND record_id=?`, account, id), &r)
	if err != nil {
		return r, err
	}
	err = s.hydrate(ctx, &r)
	return r, err
}

func (s *Store) Deltas(ctx context.Context, account string, threads, messageIDs []string) ([]Record, bool, error) {
	// Incoming provider IDs are routed to their declared namespaces before joins.
	var tids, mids []int64
	for _, value := range threads {
		entity, err := s.ResolveEntity(ctx, account, "threads", "thread_fbid:"+value)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		tids = append(tids, entity)
	}
	for _, value := range messageIDs {
		entity, err := s.ResolveEntity(ctx, account, "messages", "message_id:"+value)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, false, err
		}
		mids = append(mids, entity)
	}
	if len(tids) == 0 && len(mids) == 0 {
		return []Record{}, false, nil
	}
	where := `d.account_id=?`
	args := []any{account}
	if len(mids) > 0 {
		where += ` AND (EXISTS(SELECT 1 FROM delta_messages dm WHERE dm.version_id=d.version_id AND dm.message_entity_id IN (` + marks(len(mids)) + `)) OR (d.__typename='SlideUQPPDeleteThread' AND EXISTS(SELECT 1 FROM delta_threads dt WHERE dt.version_id=d.version_id AND dt.thread_entity_id IN (` + marks(len(tids)) + `))))`
		for _, id := range mids {
			args = append(args, id)
		}
		for _, id := range tids {
			args = append(args, id)
		}
	} else {
		where += ` AND EXISTS(SELECT 1 FROM delta_threads dt WHERE dt.version_id=d.version_id AND dt.thread_entity_id IN (` + marks(len(tids)) + `))`
		for _, id := range tids {
			args = append(args, id)
		}
	}
	rows, err := s.queryContext(ctx, `SELECT `+recordColumns+` FROM deltas_sources d WHERE `+where+` ORDER BY CAST(uq_seq_id AS INTEGER),record_id LIMIT 201`, args...)
	if err != nil {
		return nil, false, err
	}
	out := []Record{}
	for rows.Next() {
		r := Record{Table: "deltas"}
		if err = scanRecord(rows, &r); err != nil {
			rows.Close()
			return nil, false, err
		}
		out = append(out, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, false, err
	}
	more := len(out) > 200
	if more {
		out = out[:200]
	}
	for i := range out {
		if err = s.hydrate(ctx, &out[i]); err != nil {
			return nil, false, err
		}
	}
	return out, more, nil
}
func marks(n int) string {
	if n == 0 {
		return "NULL"
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// FieldVersions retrieves only versions contributing a most recently present
// field. No materialized current profile/thread or field-history table is kept.
func (s *Store) FieldVersions(ctx context.Context, r Record) ([]Record, error) {
	if r.Table != "users" && r.Table != "threads" {
		return nil, errors.New("field composition is limited to users and threads")
	}
	top := r.watermark
	if top == 0 {
		if err := s.queryRowContext(ctx, `SELECT coalesce(max(observation_id),0) FROM source_observations WHERE account_id=?`, r.Account).Scan(&top); err != nil {
			return nil, err
		}
	}
	rows, err := s.queryContext(ctx, sourceCTE(r.Table)+`, fields AS (SELECT c.version_id,c.observation_id,j.fullkey,dense_rank() OVER(PARTITION BY j.fullkey ORDER BY c.observation_id DESC) AS rank FROM candidates c,json_tree(c.residual) j WHERE c.entity_id=? AND j.type!='object'), chosen AS (SELECT DISTINCT version_id,observation_id FROM fields WHERE rank=1)
 SELECT `+recordColumns+` FROM candidates WHERE version_id IN (SELECT version_id FROM chosen) ORDER BY observation_id,version_id`, r.Account, top, r.Entity)
	if err != nil {
		return nil, err
	}
	out := []Record{}
	for rows.Next() {
		v := Record{Table: r.Table, watermark: top}
		if err = scanRecord(rows, &v); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range out {
		if s.readTx != nil {
			out[i].Raw, err = reconstruct(ctx, s.readTx, r.Account, out[i].Version)
		} else {
			out[i].Raw, err = reconstruct(ctx, s.DB, r.Account, out[i].Version)
		}
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) MessageThreadReferences(ctx context.Context, account string, version int64) ([]string, error) {
	rows, err := s.queryContext(ctx, `SELECT DISTINCT i.value FROM message_threads mt JOIN source_ids i ON i.account_id=mt.account_id AND i.thread_entity_id=mt.thread_entity_id AND i.namespace='thread.fbid' WHERE mt.account_id=? AND mt.message_version_id=?`, account, version)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var value string
		if err = rows.Scan(&value); err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, rows.Err()
}
