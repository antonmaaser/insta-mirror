package store

import (
	"context"
	"database/sql"
	"errors"
)

type Media struct {
	Account    string `json:"account_id"`
	Version    int64  `json:"attachment_version_id"`
	Record     int64  `json:"message_record_id"`
	Attachment string `json:"attachment_reference"`
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

func (s *Store) InitializeMedia() error {
	_, err := s.DB.Exec(`CREATE TABLE IF NOT EXISTS media_cache(account_id TEXT NOT NULL,attachment_version_id INTEGER NOT NULL,message_record_id INTEGER NOT NULL,attachment_reference TEXT NOT NULL,url_hash TEXT NOT NULL,status TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',mime_type TEXT NOT NULL DEFAULT '',byte_size INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,last_attempt_ms INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',PRIMARY KEY(account_id,attachment_version_id,attachment_reference),FOREIGN KEY(account_id,message_record_id) REFERENCES source_versions(account_id,version_id),FOREIGN KEY(account_id,attachment_version_id) REFERENCES source_versions(account_id,version_id));CREATE INDEX IF NOT EXISTS media_content ON media_cache(account_id,content_hash);CREATE INDEX IF NOT EXISTS media_attachment ON media_cache(account_id,attachment_reference,url_hash)`)
	return err
}
func (s *Store) SaveMedia(ctx context.Context, m Media) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO media_cache VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(account_id,attachment_version_id,attachment_reference) DO UPDATE SET url_hash=excluded.url_hash,status=excluded.status,content_hash=excluded.content_hash,mime_type=excluded.mime_type,byte_size=excluded.byte_size,attempts=excluded.attempts,last_attempt_ms=excluded.last_attempt_ms,error=excluded.error`, m.Account, m.Version, m.Record, m.Attachment, m.URLHash, m.Status, m.Hash, m.MIME, m.Size, m.Attempts, m.Attempted, m.Error)
	return err
}
func mediaScan(row interface{ Scan(...any) error }) (Media, error) {
	var m Media
	err := row.Scan(&m.Account, &m.Version, &m.Record, &m.Attachment, &m.URLHash, &m.Status, &m.Hash, &m.MIME, &m.Size, &m.Attempts, &m.Attempted, &m.Error)
	return m, err
}
func (s *Store) Media(ctx context.Context, account string, record int64) ([]Media, error) {
	rows, err := s.queryContext(ctx, mediaGraph+` SELECT m.* FROM media_cache m JOIN graph g ON g.id=m.attachment_version_id WHERE m.account_id=? ORDER BY m.attachment_reference`, account, record, account, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Media{}
	for rows.Next() {
		m, err := mediaScan(rows)
		if err != nil {
			return nil, err
		}
		if m.Hash != "" {
			m.Resource = "igdm://accounts/" + account + "/media/" + m.Hash
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
func (s *Store) CachedMedia(ctx context.Context, account, ref, urlHash string) (Media, error) {
	return mediaScan(s.queryRowContext(ctx, `SELECT * FROM media_cache WHERE account_id=? AND attachment_reference=? AND url_hash=? AND status='available' LIMIT 1`, account, ref, urlHash))
}
func (s *Store) MediaHash(ctx context.Context, account, hash string) (Media, error) {
	return mediaScan(s.queryRowContext(ctx, `SELECT * FROM media_cache WHERE account_id=? AND content_hash=? AND status='available' LIMIT 1`, account, hash))
}
func (s *Store) MessageBatch(ctx context.Context, account string, after int64) ([]Record, error) {
	rows, err := s.queryContext(ctx, `SELECT record_id,version_id,content_hash FROM messages_versions WHERE entity_id>0 AND account_id=? AND record_id>? ORDER BY record_id LIMIT 100`, account, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		r := Record{Table: "messages", Account: account}
		if err = rows.Scan(&r.RowID, &r.Version, &r.ContentHash); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i := range out {
		out[i].Raw, err = reconstruct(ctx, s.DB, account, out[i].Version)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
func NoMedia(err error) bool { return errors.Is(err, sql.ErrNoRows) }

const mediaGraph = `WITH RECURSIVE graph(id) AS (SELECT version_id FROM source_versions WHERE account_id=? AND version_id=? UNION SELECT r.child_id FROM source_relations r JOIN graph g ON g.id=r.parent_id WHERE r.account_id=? AND r.child_id IS NOT NULL AND r.field IN ('content','xma','attachments','attachment','videos','ordered_photo_video_attachments','audio_attachments','animated_media','preview_image','xmaPreviewImage','header_icon'))`

// Anchor downloads to shared attachment content, so changes to a sender profile
// do not allocate another cache record for the same attachment version.
func (s *Store) AttachmentVersion(ctx context.Context, account string, message int64, ref, url string) (int64, error) {
	var id int64
	err := s.queryRowContext(ctx, mediaGraph+` SELECT v.version_id FROM graph g JOIN source_versions v ON v.version_id=g.id WHERE v.account_id=? AND
 (? NOT LIKE 'attachment_fbid:%' OR json_extract(v.residual,'$.attachment_fbid')=substr(?,17)) AND ?!='' AND ? IN (
 json_extract(v.residual,'$.preview_url'),json_extract(v.residual,'$.url'),json_extract(v.residual,'$.attachment_mp4_url'),json_extract(v.residual,'$.attachment_webp_url'),json_extract(v.residual,'$.attachment_cdn_url'),json_extract(v.residual,'$.attachment_cdn_fallback_url')) ORDER BY v.version_id LIMIT 1`, account, message, account, account, ref, ref, url, url).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return message, nil
	}
	return id, err
}
