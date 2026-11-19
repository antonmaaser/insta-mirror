package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// CanonicalMediaURL is an identity input, never a download endpoint.
func CanonicalMediaURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return "", errors.New("invalid media URL")
	}
	host := strings.ToLower(u.Hostname())
	if strings.HasPrefix(host, "scontent-") && strings.HasSuffix(host, ".cdninstagram.com") {
		host = "cdninstagram.com"
	}
	u.Host = host
	if port := u.Port(); port != "" && port != "443" {
		return "", errors.New("unsupported media port")
	}
	u.Fragment = ""
	q := u.Query()
	for _, key := range []string{"oh", "oe", "_nc_gid", "_nc_ohc", "_nc_oc", "_nc_ht", "_nc_sid", "_nc_cat", "_nc_ss", "_nc_ct", "_nc_map", "ccb", "efg"} {
		q.Del(key)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func MediaKey(fbid string, urls []string) (string, error) {
	if fbid != "" {
		return "fbid:" + fbid, nil
	}
	for _, raw := range urls {
		if u, err := url.Parse(raw); err == nil {
			if key := u.Query().Get("ig_cache_key"); key != "" {
				return "ig_cache:" + key, nil
			}
		}
	}
	if len(urls) == 0 {
		return "", errors.New("media has no source identity")
	}
	canonical, err := CanonicalMediaURL(urls[0])
	if err != nil {
		return "", err
	}
	return "path_sha256:" + Hash([]byte(canonical)), nil
}

func EnsureAsset(ctx context.Context, tx *sql.Tx, account, key, kind string, urls []string, restricted bool) (int64, error) {
	status := "pending"
	if restricted || len(urls) == 0 {
		status = "unavailable"
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO media_assets(account_id,canonical_key,asset_kind,download_status) VALUES(?,?,?,?) ON CONFLICT(account_id,canonical_key) DO NOTHING`, account, key, kind, status)
	if err != nil {
		return 0, err
	}
	var id int64
	var existing string
	if err = tx.QueryRowContext(ctx, `SELECT asset_id,download_status FROM media_assets WHERE account_id=? AND canonical_key=?`, account, key).Scan(&id, &existing); err != nil {
		return 0, err
	}
	if existing == "available" || restricted {
		return id, nil
	}
	changed := false
	for i, raw := range urls {
		if i > 1 {
			break
		}
		if raw == "" {
			continue
		}
		role := "primary"
		if i == 1 {
			role = "fallback"
		}
		var expires any
		if u, e := url.Parse(raw); e == nil {
			if hex := u.Query().Get("oe"); hex != "" {
				if n, e := strconv.ParseInt(hex, 16, 64); e == nil {
					expires = n * 1000
				}
			}
		}
		res, e := tx.ExecContext(ctx, `INSERT INTO media_asset_urls(account_id,asset_id,url_role,url,expires_at_ms,observed_at_ms) VALUES(?,?,?,?,?,?) ON CONFLICT(account_id,asset_id,url_role) DO UPDATE SET url=excluded.url,expires_at_ms=excluded.expires_at_ms,observed_at_ms=excluded.observed_at_ms WHERE url IS NOT excluded.url`, account, id, role, raw, expires, time.Now().UnixMilli())
		if e != nil {
			return 0, e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return 0, e
		}
		changed = changed || n > 0
	}
	if changed {
		_, err = tx.ExecContext(ctx, `UPDATE media_assets SET download_status='pending',last_attempt_ms=NULL,last_error=NULL WHERE account_id=? AND asset_id=? AND download_status<>'available'`, account, id)
	}
	return id, err
}

type Media struct {
	Account    string   `json:"account_id"`
	AssetID    int64    `json:"asset_id"`
	MessageID  int64    `json:"message_id,omitempty"`
	Attachment string   `json:"attachment_key"`
	Status     string   `json:"status"`
	Hash       string   `json:"content_hash,omitempty"`
	MIME       string   `json:"mime_type,omitempty"`
	Size       int64    `json:"byte_size,omitempty"`
	Attempts   int      `json:"attempts"`
	Attempted  int64    `json:"last_attempt_ms"`
	Error      string   `json:"error,omitempty"`
	Resource   string   `json:"resource_uri,omitempty"`
	URLs       []string `json:"-"`
}

const mediaColumns = `a.account_id,a.asset_id,a.canonical_key,a.download_status,coalesce(a.content_sha256,''),coalesce(a.mime_type,''),coalesce(a.byte_size,0),coalesce(a.last_attempt_ms,0),a.attempts,coalesce(a.last_error,'')`

func mediaScan(row interface{ Scan(...any) error }) (Media, error) {
	var m Media
	err := row.Scan(&m.Account, &m.AssetID, &m.Attachment, &m.Status, &m.Hash, &m.MIME, &m.Size, &m.Attempted, &m.Attempts, &m.Error)
	if m.Hash != "" {
		m.Resource = "igdm://accounts/" + m.Account + "/media/" + m.Hash
	}
	return m, err
}
func (s *Store) SaveMedia(ctx context.Context, m Media) error {
	_, err := s.DB.ExecContext(ctx, `UPDATE media_assets SET download_status=?,content_sha256=?,mime_type=?,byte_size=?,last_attempt_ms=?,attempts=?,last_error=? WHERE account_id=? AND asset_id=? AND download_status<>'available'`, m.Status, m.Hash, m.MIME, m.Size, m.Attempted, m.Attempts, m.Error, m.Account, m.AssetID)
	return err
}
func (s *Store) PendingMedia(ctx context.Context, account string, after int64) ([]Media, error) {
	rows, err := s.queryContext(ctx, `SELECT `+mediaColumns+` FROM media_assets a WHERE a.account_id=? AND a.asset_id>? AND a.download_status='pending' AND coalesce(a.last_attempt_ms,0)<? AND EXISTS(SELECT 1 FROM media_asset_urls u WHERE u.account_id=a.account_id AND u.asset_id=a.asset_id) ORDER BY a.asset_id LIMIT 100`, account, after, time.Now().Add(-time.Hour).UnixMilli())
	if err != nil {
		return nil, err
	}
	var out []Media
	for rows.Next() {
		m, e := mediaScan(rows)
		if e != nil {
			rows.Close()
			return nil, e
		}
		out = append(out, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for i := range out {
		rows, e := s.queryContext(ctx, `SELECT url FROM media_asset_urls WHERE account_id=? AND asset_id=? ORDER BY CASE url_role WHEN 'primary' THEN 0 ELSE 1 END`, account, out[i].AssetID)
		if e != nil {
			return nil, e
		}
		for rows.Next() {
			var raw string
			if e = rows.Scan(&raw); e != nil {
				rows.Close()
				return nil, e
			}
			out[i].URLs = append(out[i].URLs, raw)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (s *Store) Media(ctx context.Context, account string, message int64) ([]Media, error) {
	rows, err := s.queryContext(ctx, `SELECT `+mediaColumns+` FROM message_attachments x JOIN media_assets a ON a.account_id=x.account_id AND a.asset_id=x.asset_id JOIN messages m ON m.account_id=x.account_id AND m.message_id=x.message_id WHERE x.account_id=? AND x.message_id=? AND m.deleted=0 AND m.is_placeholder=0 ORDER BY x.ordinal`, account, message)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Media
	for rows.Next() {
		m, e := mediaScan(rows)
		if e != nil {
			return nil, e
		}
		m.MessageID = message
		out = append(out, m)
	}
	return out, rows.Err()
}
func (s *Store) MediaHash(ctx context.Context, account, hash string) (Media, error) {
	return mediaScan(s.queryRowContext(ctx, `SELECT `+mediaColumns+` FROM media_assets a WHERE a.account_id=? AND a.content_sha256=? AND a.download_status='available' AND (EXISTS(SELECT 1 FROM message_attachments x JOIN messages m ON m.account_id=x.account_id AND m.message_id=x.message_id WHERE x.account_id=a.account_id AND x.asset_id=a.asset_id AND m.deleted=0 AND m.is_placeholder=0) OR EXISTS(SELECT 1 FROM users u WHERE u.account_id=a.account_id AND u.avatar_asset_id=a.asset_id) OR EXISTS(SELECT 1 FROM threads t WHERE t.account_id=a.account_id AND t.image_asset_id=a.asset_id AND t.deleted=0)) LIMIT 1`, account, hash))
}

type MediaListing struct {
	Hash, MIME string
	Size       int64
	Sender     string
}

func (s *Store) ListMedia(ctx context.Context, account string) ([]MediaListing, error) {
	rows, err := s.queryContext(ctx, `SELECT DISTINCT a.content_sha256,a.mime_type,a.byte_size,coalesce(nullif(u.full_name,''),nullif(u.username,''),'Unknown sender') FROM media_assets a JOIN message_attachments x ON x.account_id=a.account_id AND x.asset_id=a.asset_id JOIN messages m ON m.account_id=x.account_id AND m.message_id=x.message_id LEFT JOIN users u ON u.account_id=m.account_id AND u.user_id=m.sender_id WHERE a.account_id=? AND a.download_status='available' AND m.is_placeholder=0 AND m.deleted=0 ORDER BY a.content_sha256`, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MediaListing
	for rows.Next() {
		var m MediaListing
		if err = rows.Scan(&m.Hash, &m.MIME, &m.Size, &m.Sender); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
func NoMedia(err error) bool { return errors.Is(err, sql.ErrNoRows) }
