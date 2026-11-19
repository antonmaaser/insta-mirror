package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strings"
)

// Attachments and Edits assemble presentation values from normalized children.
func (s *Store) Attachments(ctx context.Context, account string, id int64) ([]Attachment, error) {
	rows, err := s.queryContext(ctx, `SELECT a.canonical_key,x.attachment_kind,x.asset_id,x.ordinal,x.remote_entity_id,x.remote_entity_type,x.title,x.subtitle,x.target_url,a.download_status FROM message_attachments x JOIN media_assets a ON a.account_id=x.account_id AND a.asset_id=x.asset_id WHERE x.account_id=? AND x.message_id=? ORDER BY x.ordinal`, account, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Attachment{}
	for rows.Next() {
		var a Attachment
		var status string
		if err = rows.Scan(&a.Key, &a.Kind, &a.AssetID, &a.Ordinal, &a.RemoteEntityID, &a.RemoteEntityType, &a.Title, &a.Subtitle, &a.TargetURL, &status); err != nil {
			return nil, err
		}
		a.Restricted = status == "unavailable"
		out = append(out, a)
	}
	return out, rows.Err()
}
func (s *Store) Edits(ctx context.Context, account string, id int64) ([]Edit, error) {
	rows, err := s.queryContext(ctx, `SELECT previous_body,edited_at_ms FROM message_edits WHERE account_id=? AND message_id=? ORDER BY edit_index`, account, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Edit{}
	for rows.Next() {
		var e Edit
		if err = rows.Scan(&e.Body, &e.At); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (s *Store) EditsFromTx(ctx context.Context, tx *sql.Tx, account string, id int64) ([]Edit, error) {
	return (&Store{readTx: tx}).Edits(ctx, account, id)
}

func ReplaceAttachments(ctx context.Context, tx *sql.Tx, account string, id int64, items []Attachment) error {
	for i, a := range items {
		kind := a.Kind
		switch {
		case strings.Contains(strings.ToLower(kind), "audio"):
			kind = "AUDIO"
		case strings.Contains(strings.ToLower(kind), "video"):
			kind = "VIDEO"
		case strings.Contains(strings.ToLower(kind), "xma"):
			kind = "XMA_PREVIEW"
		default:
			kind = "IMAGE"
		}
		if a.Key == "" {
			return errors.New("attachment lacks stable identity")
		}
		asset, err := EnsureAsset(ctx, tx, account, a.Key, kind, a.URLs, a.Restricted)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO message_attachments(account_id,message_id,ordinal,asset_id,attachment_kind,remote_entity_id,remote_entity_type,title,subtitle,target_url) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,message_id,ordinal) DO UPDATE SET asset_id=excluded.asset_id,attachment_kind=excluded.attachment_kind,remote_entity_id=excluded.remote_entity_id,remote_entity_type=excluded.remote_entity_type,title=excluded.title,subtitle=excluded.subtitle,target_url=excluded.target_url WHERE asset_id IS NOT excluded.asset_id OR attachment_kind IS NOT excluded.attachment_kind OR remote_entity_id IS NOT excluded.remote_entity_id OR remote_entity_type IS NOT excluded.remote_entity_type OR title IS NOT excluded.title OR subtitle IS NOT excluded.subtitle OR target_url IS NOT excluded.target_url`, account, id, i, asset, a.Kind, a.RemoteEntityID, a.RemoteEntityType, a.Title, a.Subtitle, a.TargetURL)
		if err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM message_attachments WHERE account_id=? AND message_id=? AND ordinal>=?`, account, id, len(items))
	return err
}
func ReplaceEdits(ctx context.Context, tx *sql.Tx, account string, id int64, items []Edit) error {
	for i, e := range items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO message_edits VALUES(?,?,?,?,?) ON CONFLICT(account_id,message_id,edit_index) DO UPDATE SET edited_at_ms=excluded.edited_at_ms,previous_body=excluded.previous_body WHERE edited_at_ms IS NOT excluded.edited_at_ms OR previous_body IS NOT excluded.previous_body`, account, id, i, e.At, e.Body); err != nil {
			return err
		}
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM message_edits WHERE account_id=? AND message_id=? AND edit_index>=?`, account, id, len(items))
	return err
}

type canonicalField struct {
	Name  string
	Value any
}
type canonicalEntity struct {
	Fields   []canonicalField
	Children []canonicalCollection
}
type canonicalCollection struct {
	Name string
	Rows [][]canonicalField
}

func canonicalRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([][]canonicalField, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := [][]canonicalField{}
	for rows.Next() {
		values := make([]any, len(names))
		dest := make([]any, len(names))
		for i := range dest {
			dest[i] = &values[i]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		fields := []canonicalField{}
		for i, name := range names {
			if name == "fingerprint" || name == "updated_at_ms" || name == "source_tier" {
				continue
			}
			fields = append(fields, canonicalField{name, values[i]})
		}
		out = append(out, fields)
	}
	return out, rows.Err()
}

// CanonicalContent includes persistent source state, never URL signatures or cache bookkeeping.
func CanonicalContent(ctx context.Context, tx *sql.Tx, account, kind string, id int64) ([]byte, [32]byte, error) {
	return CanonicalMergedContent(ctx, tx, account, kind, id, nil)
}
func CanonicalMergedContent(ctx context.Context, tx *sql.Tx, account, kind string, id int64, merged map[string]any) ([]byte, [32]byte, error) {
	rows, err := canonicalRows(ctx, tx, "SELECT * FROM "+kind+" WHERE account_id=? AND "+aliasColumn(kind)+"=?", account, id)
	if err != nil {
		return nil, [32]byte{}, err
	}
	if len(rows) == 0 && merged == nil {
		return nil, [32]byte{}, sql.ErrNoRows
	}
	content := canonicalEntity{}
	if len(rows) > 0 {
		content.Fields = rows[0]
	} else {
		for name, value := range merged {
			if name == "fingerprint" || name == "source_tier" || name == "updated_at_ms" {
				continue
			}
			content.Fields = append(content.Fields, canonicalField{name, value})
		}
	}
	sort.Slice(content.Fields, func(i, j int) bool { return content.Fields[i].Name < content.Fields[j].Name })
	for i := range content.Fields {
		if value, ok := merged[content.Fields[i].Name]; ok {
			content.Fields[i].Value = value
		}
	}
	var children []struct{ name, query string }
	switch kind {
	case "threads":
		children = []struct{ name, query string }{{"members", `SELECT * FROM thread_members WHERE account_id=? AND thread_id=? ORDER BY user_id`}, {"pins", `SELECT message_id FROM thread_pinned_messages WHERE account_id=? AND thread_id=? ORDER BY message_id`}}
	case "messages":
		children = []struct{ name, query string }{{"attachments", `SELECT ordinal,asset_id,attachment_kind,remote_entity_id,remote_entity_type,title,subtitle,target_url FROM message_attachments WHERE account_id=? AND message_id=? ORDER BY ordinal`}, {"reactions", `SELECT user_id,reaction,reacted_at_ms,log_message_id FROM message_reactions WHERE account_id=? AND message_id=? ORDER BY user_id`}, {"mentions", `SELECT offset,length,kind,mentioned_user_id,mentioned_thread_id FROM message_mentions WHERE account_id=? AND message_id=? ORDER BY offset,length,kind,mentioned_user_id,mentioned_thread_id`}, {"edits", `SELECT edit_index,edited_at_ms,previous_body FROM message_edits WHERE account_id=? AND message_id=? ORDER BY edit_index`}}
	}
	for _, child := range children {
		rows, err := canonicalRows(ctx, tx, child.query, account, id)
		if err != nil {
			return nil, [32]byte{}, err
		}
		content.Children = append(content.Children, canonicalCollection{child.name, rows})
	}
	raw, err := json.Marshal(content)
	return raw, sha256.Sum256(raw), err
}
