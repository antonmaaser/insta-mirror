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
	Sender   string `json:"sender_id,omitempty"`
	AfterMS  int64  `json:"after_ms,omitempty"`
	BeforeMS int64  `json:"before_ms,omitempty"`
	Text     string `json:"text,omitempty"`
	Cursor   string `json:"cursor,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

type Page struct {
	Records []Record
	Next    string
}

type Record struct {
	ID        int64    `json:"id"`
	Account   string   `json:"account_id"`
	Kind      string   `json:"kind"`
	Reference string   `json:"reference"`
	User      *User    `json:"user,omitempty"`
	Thread    *Thread  `json:"thread,omitempty"`
	Message   *Message `json:"message,omitempty"`
}

type User struct {
	Username          *string `json:"username,omitempty"`
	Name              *string `json:"name,omitempty"`
	AvatarURL         *string `json:"avatar_url,omitempty"`
	Verified          *bool   `json:"verified,omitempty"`
	Restricted        *bool   `json:"restricted,omitempty"`
	Blocking          *bool   `json:"blocking,omitempty"`
	Following         *bool   `json:"following,omitempty"`
	FollowedBy        *bool   `json:"followed_by,omitempty"`
	AIAgentType       *string `json:"ai_agent_type,omitempty"`
	LatestReel        *int64  `json:"latest_reel,omitempty"`
	LatestBestiesReel *int64  `json:"latest_besties_reel,omitempty"`
	ReelSeenAt        *int64  `json:"reel_seen_at,omitempty"`
}

type Thread struct {
	Title        *string  `json:"title,omitempty"`
	ImageURL     *string  `json:"image_url,omitempty"`
	Folder       *string  `json:"folder,omitempty"`
	Subtype      *string  `json:"subtype,omitempty"`
	ActivityAt   int64    `json:"activity_at_ms"`
	ReadAt       *int64   `json:"read_at_ms,omitempty"`
	InputMode    *int64   `json:"input_mode,omitempty"`
	Reachability *string  `json:"reachability,omitempty"`
	Group        *bool    `json:"group,omitempty"`
	Muted        *bool    `json:"muted,omitempty"`
	Pinned       *bool    `json:"pinned,omitempty"`
	Unread       *bool    `json:"unread,omitempty"`
	Deleted      bool     `json:"deleted,omitempty"`
	Members      []Member `json:"members,omitempty"`
}

type Member struct {
	UserID   int64   `json:"user_id"`
	Name     string  `json:"name,omitempty"`
	Nickname *string `json:"nickname,omitempty"`
	Admin    *bool   `json:"admin,omitempty"`
	ReadAt   *int64  `json:"read_at_ms,omitempty"`
}

type Message struct {
	ThreadID      *int64       `json:"thread_id,omitempty"`
	SenderID      *int64       `json:"sender_id,omitempty"`
	Sender        string       `json:"sender,omitempty"`
	Reply         *Reply       `json:"reply,omitempty"`
	SentAt        int64        `json:"sent_at_ms"`
	Body          *string      `json:"body,omitempty"`
	Kind          *string      `json:"content_type,omitempty"`
	Snippet       *string      `json:"snippet,omitempty"`
	Attachments   []Attachment `json:"attachments,omitempty"`
	Edits         []Edit       `json:"edits,omitempty"`
	EditedAt      *int64       `json:"edited_at_ms,omitempty"`
	ExpiresAt     *int64       `json:"expires_at_ms,omitempty"`
	ViewExpiresAt *int64       `json:"view_expires_at_ms,omitempty"`
	Forwarded     *bool        `json:"forwarded,omitempty"`
	Pinned        *bool        `json:"pinned,omitempty"`
	Reported      *bool        `json:"reported,omitempty"`
	AIGenerated   *bool        `json:"ai_generated,omitempty"`
	Deleted       bool         `json:"deleted,omitempty"`
	Placeholder   bool         `json:"placeholder,omitempty"`
	Reactions     []Reaction   `json:"reactions,omitempty"`
	Mentions      []Mention    `json:"mentions,omitempty"`
}

type Reply struct {
	ID        int64   `json:"message_id"`
	Available bool    `json:"available"`
	Body      *string `json:"body,omitempty"`
}

type Reaction struct {
	UserID       int64  `json:"user_id"`
	Value        string `json:"reaction"`
	At           *int64 `json:"reacted_at_ms,omitempty"`
	LogMessageID *int64 `json:"log_message_id,omitempty"`
}

type Mention struct {
	Offset   int    `json:"offset"`
	Length   int    `json:"length"`
	Kind     string `json:"kind"`
	UserID   *int64 `json:"user_id,omitempty"`
	ThreadID *int64 `json:"thread_id,omitempty"`
}

func EntityRef(account, kind string, id int64) string {
	return fmt.Sprintf("igdm://accounts/%s/entities/%s/%d", account, kind, id)
}

func (s *Store) ResolveEntity(ctx context.Context, account, kind, ref string) (int64, error) {
	local := strings.TrimPrefix(ref, "entity:")
	if id, err := strconv.ParseInt(local, 10, 64); err == nil && id > 0 {
		return id, nil
	}
	field, value, ok := strings.Cut(ref, ":")
	if !ok || value == "" {
		return 0, errors.New("expected a local ID or namespaced source ID")
	}
	var ns, scope string
	switch kind {
	case "users":
		ns = map[string]string{"id": "user.igid", "pk": "user.igid", "igid": "user.igid", "interop_messaging_user_fbid": "user.messaging_fbid", "sender_fbid": "user.messaging_fbid", "fbid_v2": "user.fbid_v2"}[field]
	case "threads":
		ns = map[string]string{"id": "thread.fbid", "thread_fbid": "thread.fbid", "thread_id": "thread.igid", "thread_key": "thread.key"}[field]
		if ns == "thread.key" {
			scope, value, ok = strings.Cut(value, ":")
			if !ok || scope == "" || value == "" {
				return 0, errors.New("use thread_key:SUBTYPE:VALUE")
			}
		}
	case "messages":
		if field == "id" || field == "message_id" {
			ns = "message.mid"
		}
	}
	if ns == "" {
		return 0, errors.New("unsupported source ID namespace")
	}
	var id int64
	err := s.queryRowContext(ctx, "SELECT "+aliasColumn(kind)+" FROM source_ids WHERE account_id=? AND namespace=? AND scope=? AND remote_id=?", account, ns, scope, value).Scan(&id)
	return id, err
}

type position struct {
	Stamp, ID int64
	Query     string
}

func (s *Store) Query(ctx context.Context, kind string, q Query) (Page, error) {
	return s.readPage(ctx, kind, q, false)
}

func (s *Store) readPage(ctx context.Context, kind string, q Query, ascending bool) (Page, error) {
	page := Page{Records: []Record{}}
	limit := q.Limit
	if limit == 0 {
		limit = 30
	}
	if limit < 1 || limit > 100 {
		return page, errors.New("limit must be between 1 and 100")
	}
	if q.Account == "" {
		return page, errors.New("account_id is required")
	}
	var selectSQL, order string
	switch kind {
	case "users":
		selectSQL = `SELECT e.id,e.username,e.name,e.avatar_url,e.verified,e.restricted,e.blocking,e.following,e.followed_by,e.ai_agent_type,e.latest_reel,e.latest_besties_reel,e.reel_seen_at FROM users e`
		order = "0"
	case "threads":
		selectSQL = `SELECT e.id,e.title,e.image_url,e.folder,e.subtype,coalesce(e.activity_at,0),e.read_at,e.input_mode,e.reachability,e.is_group,e.muted,e.pinned,e.unread,coalesce(e.deleted,0) FROM threads e`
		order = "coalesce(e.activity_at,0)"
	case "messages":
		selectSQL = `SELECT e.id,e.thread_id,e.sender_id,coalesce(u.name,u.username,''),e.reply_id,
 coalesce(r.placeholder=0 AND coalesce(r.deleted,0)=0,0),CASE WHEN r.placeholder=0 AND coalesce(r.deleted,0)=0 THEN r.body END,
 coalesce(e.sent_at,0),e.body,e.kind,e.snippet,coalesce(e.attachments,'[]'),coalesce(e.edits,'[]'),e.edited_at,e.expires_at,e.view_expires_at,
 e.forwarded,e.pinned,e.reported,e.ai_generated,coalesce(e.deleted,0),e.placeholder
 FROM messages e LEFT JOIN users u ON u.id=e.sender_id LEFT JOIN messages r ON r.id=e.reply_id`
		order = "e.sent_at"
	default:
		return page, errors.New("unsupported entity kind")
	}
	var where = []string{"e.account_id=?"}
	args := []any{q.Account}
	filter := func(clause string, value any) { where = append(where, clause); args = append(args, value) }
	if q.SourceID != "" {
		id, err := s.ResolveEntity(ctx, q.Account, kind, q.SourceID)
		if errors.Is(err, sql.ErrNoRows) {
			return page, nil
		}
		if err != nil {
			return page, err
		}
		if q.EntityID != 0 && q.EntityID != id {
			return page, ErrConflict
		}
		q.EntityID = id
	}
	if q.EntityID != 0 {
		filter("e.id=?", q.EntityID)
	}
	if kind == "threads" && q.EntityID == 0 {
		where = append(where, "coalesce(e.deleted,0)=0")
	}
	if kind == "messages" {
		if q.EntityID == 0 {
			where = append(where, "e.placeholder=0")
		}
		if q.ThreadID != "" {
			id, err := s.ResolveEntity(ctx, q.Account, "threads", q.ThreadID)
			if errors.Is(err, sql.ErrNoRows) {
				return page, nil
			}
			if err != nil {
				return page, err
			}
			filter("e.thread_id=?", id)
		}
		if q.Sender != "" {
			id, err := s.ResolveEntity(ctx, q.Account, "users", q.Sender)
			if errors.Is(err, sql.ErrNoRows) {
				return page, nil
			}
			if err != nil {
				return page, err
			}
			filter("e.sender_id=?", id)
		}
		if q.Text != "" {
			filter("instr(lower(coalesce(e.body,e.snippet,'')),lower(?))>0", q.Text)
		}
		if q.AfterMS != 0 {
			filter("e.sent_at>=?", q.AfterMS)
		}
		if q.BeforeMS != 0 {
			filter("e.sent_at<?", q.BeforeMS)
		}
	}
	cursor := q.Cursor
	q.Cursor, q.Limit = "", 0
	hash := Hash([]byte(kind + strconv.FormatBool(ascending) + JSONValue(q)))
	direction, op := "DESC", "<"
	if ascending {
		direction, op = "ASC", ">"
	}
	if cursor != "" {
		var pos position
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || json.Unmarshal(raw, &pos) != nil || pos.Query != hash || pos.ID <= 0 {
			return page, errors.New("invalid cursor for this query")
		}
		where = append(where, "("+order+",e.id) "+op+" (?,?)")
		args = append(args, pos.Stamp, pos.ID)
	}
	orderBy := "e.id " + direction
	if kind != "users" {
		orderBy = order + " " + direction + "," + orderBy
	}
	rows, err := s.queryContext(ctx, selectSQL+" WHERE "+strings.Join(where, " AND ")+" ORDER BY "+orderBy+" LIMIT ?", append(args, limit+1)...)
	if err != nil {
		return page, err
	}
	for rows.Next() {
		r, err := scanEntity(rows, q.Account, kind)
		if err != nil {
			rows.Close()
			return page, err
		}
		page.Records = append(page.Records, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return page, err
	}
	if len(page.Records) > limit {
		page.Records = page.Records[:limit]
		last := page.Records[limit-1]
		pos := position{ID: last.ID, Query: hash}
		if last.Thread != nil {
			pos.Stamp = last.Thread.ActivityAt
		}
		if last.Message != nil {
			pos.Stamp = last.Message.SentAt
		}
		page.Next = base64.RawURLEncoding.EncodeToString([]byte(JSONValue(pos)))
	}
	if err = s.loadLinks(ctx, kind, page.Records); err != nil {
		return page, err
	}
	return page, nil
}

func scanEntity(rows *sql.Rows, account, kind string) (Record, error) {
	r := Record{Account: account, Kind: kind}
	var err error
	switch kind {
	case "users":
		u := &User{}
		err = rows.Scan(&r.ID, &u.Username, &u.Name, &u.AvatarURL, &u.Verified, &u.Restricted, &u.Blocking, &u.Following, &u.FollowedBy, &u.AIAgentType, &u.LatestReel, &u.LatestBestiesReel, &u.ReelSeenAt)
		r.User = u
	case "threads":
		t := &Thread{}
		err = rows.Scan(&r.ID, &t.Title, &t.ImageURL, &t.Folder, &t.Subtype, &t.ActivityAt, &t.ReadAt, &t.InputMode, &t.Reachability, &t.Group, &t.Muted, &t.Pinned, &t.Unread, &t.Deleted)
		r.Thread = t
	case "messages":
		m := &Message{}
		var replyID *int64
		var reply Reply
		var attachments, edits []byte
		err = rows.Scan(&r.ID, &m.ThreadID, &m.SenderID, &m.Sender, &replyID, &reply.Available, &reply.Body, &m.SentAt, &m.Body, &m.Kind, &m.Snippet, &attachments, &edits, &m.EditedAt, &m.ExpiresAt, &m.ViewExpiresAt, &m.Forwarded, &m.Pinned, &m.Reported, &m.AIGenerated, &m.Deleted, &m.Placeholder)
		if err == nil {
			err = json.Unmarshal(attachments, &m.Attachments)
		}
		if err == nil {
			err = json.Unmarshal(edits, &m.Edits)
		}
		if replyID != nil {
			reply.ID = *replyID
			m.Reply = &reply
		}
		r.Message = m
	}
	r.Reference = EntityRef(account, kind, r.ID)
	return r, err
}

func (s *Store) loadLinks(ctx context.Context, kind string, records []Record) error {
	if len(records) == 0 || kind == "users" {
		return nil
	}
	args := make([]any, 0, len(records))
	index := make(map[int64]Record, len(records))
	for _, r := range records {
		if r.Message != nil && (r.Message.Deleted || r.Message.Placeholder) {
			continue
		}
		args = append(args, r.ID)
		index[r.ID] = r
	}
	if len(args) == 0 {
		return nil
	}
	in := " IN (" + strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + ")"
	if kind == "threads" {
		rows, err := s.queryContext(ctx, `SELECT m.thread_id,m.user_id,coalesce(u.name,u.username,''),m.nickname,m.admin,m.read_at
 FROM thread_members m JOIN users u ON u.id=m.user_id WHERE m.member=1 AND m.thread_id`+in+" ORDER BY m.thread_id,m.user_id", args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			var m Member
			if err = rows.Scan(&id, &m.UserID, &m.Name, &m.Nickname, &m.Admin, &m.ReadAt); err != nil {
				return err
			}
			t := index[id].Thread
			t.Members = append(t.Members, m)
		}
		return rows.Err()
	}
	rows, err := s.queryContext(ctx, `SELECT message_id,user_id,reaction,reacted_at,log_message_id FROM message_reactions WHERE message_id`+in+" ORDER BY message_id,user_id", args...)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		var r Reaction
		if err = rows.Scan(&id, &r.UserID, &r.Value, &r.At, &r.LogMessageID); err != nil {
			rows.Close()
			return err
		}
		m := index[id].Message
		m.Reactions = append(m.Reactions, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = s.queryContext(ctx, `SELECT message_id,offset,length,kind,user_id,thread_id FROM message_mentions WHERE message_id`+in+" ORDER BY message_id,offset,length,kind", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var item Mention
		if err = rows.Scan(&id, &item.Offset, &item.Length, &item.Kind, &item.UserID, &item.ThreadID); err != nil {
			return err
		}
		m := index[id].Message
		m.Mentions = append(m.Mentions, item)
	}
	return rows.Err()
}

type ChatRow struct {
	Identity string
	Title    string
	Members  []Member
	Activity int64
}

func (s *Store) Chats(ctx context.Context, account string) ([]ChatRow, error) {
	var chats []ChatRow
	q := Query{Account: account, Limit: 100}
	for {
		page, err := s.Query(ctx, "threads", q)
		if err != nil {
			return nil, err
		}
		for _, r := range page.Records {
			row := ChatRow{Identity: strconv.FormatInt(r.ID, 10), Members: r.Thread.Members, Activity: r.Thread.ActivityAt}
			if r.Thread.Title != nil {
				row.Title = *r.Thread.Title
			}
			chats = append(chats, row)
		}
		if page.Next == "" {
			return chats, nil
		}
		q.Cursor = page.Next
	}
}

func (s *Store) TranscriptMessages(ctx context.Context, account string, thread int64, visit func(Record) error) error {
	q := Query{Account: account, ThreadID: strconv.FormatInt(thread, 10), Limit: 100}
	for {
		page, err := s.readPage(ctx, "messages", q, true)
		if err != nil {
			return err
		}
		for _, r := range page.Records {
			if err = visit(r); err != nil {
				return err
			}
		}
		if page.Next == "" {
			return nil
		}
		q.Cursor = page.Next
	}
}
