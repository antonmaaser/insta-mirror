package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

type Media struct {
	Account    string `json:"account_id"`
	MessageID  int64  `json:"message_id"`
	Attachment string `json:"attachment_key"`
	URLHash    string `json:"-"`
	Status     string `json:"status"`
	Hash       string `json:"content_hash,omitempty"`
	MIME       string `json:"mime_type,omitempty"`
	Size       int64  `json:"byte_size,omitempty"`
	Attempts   int    `json:"attempts"`
	Attempted  int64  `json:"last_attempt_ms"`
	Error      string `json:"error,omitempty"`
	Resource   string `json:"resource_uri,omitempty"`
}

func (s *Store) SaveMedia(ctx context.Context, m Media, attachments string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO media_cache
 SELECT ?,?,?,?,?,?,?,?,?,?,? WHERE EXISTS(SELECT 1 FROM messages WHERE id=? AND account_id=? AND coalesce(deleted,0)=0 AND attachments=?)
 ON CONFLICT(account_id,message_id,attachment) DO UPDATE SET url_hash=excluded.url_hash,status=excluded.status,
 hash=excluded.hash,mime=excluded.mime,size=excluded.size,attempted=excluded.attempted,attempts=excluded.attempts,error=excluded.error`,
		m.Account, m.MessageID, m.Attachment, m.URLHash, m.Status, m.Hash, m.MIME, m.Size, m.Attempted, m.Attempts, m.Error, m.MessageID, m.Account, attachments)
	return err
}

func mediaScan(row interface{ Scan(...any) error }) (Media, error) {
	var m Media
	err := row.Scan(&m.Account, &m.MessageID, &m.Attachment, &m.URLHash, &m.Status, &m.Hash, &m.MIME, &m.Size, &m.Attempted, &m.Attempts, &m.Error)
	if m.Hash != "" {
		m.Resource = "igdm://accounts/" + m.Account + "/media/" + m.Hash
	}
	return m, err
}

func (s *Store) Media(ctx context.Context, account string, message int64) ([]Media, error) {
	rows, err := s.queryContext(ctx, `SELECT * FROM media_cache WHERE account_id=? AND message_id=? ORDER BY attachment`, account, message)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Media
	for rows.Next() {
		m, err := mediaScan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) MediaHash(ctx context.Context, account, hash string) (Media, error) {
	return mediaScan(s.queryRowContext(ctx, `SELECT * FROM media_cache WHERE account_id=? AND hash=? AND status='available' LIMIT 1`, account, hash))
}

type MediaMessage struct {
	ID                int64
	Attachments       []Attachment
	StoredAttachments string
}

func (s *Store) MessageBatch(ctx context.Context, account string, after int64) ([]MediaMessage, error) {
	rows, err := s.queryContext(ctx, `SELECT id,attachments FROM messages WHERE account_id=? AND id>? AND placeholder=0 AND coalesce(deleted,0)=0 AND attachments NOT IN ('[]','null') ORDER BY id LIMIT 100`, account, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MediaMessage
	for rows.Next() {
		var m MediaMessage
		if err = rows.Scan(&m.ID, &m.StoredAttachments); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(m.StoredAttachments), &m.Attachments); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func NoMedia(err error) bool { return errors.Is(err, sql.ErrNoRows) }
