package mcp

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"go.mau.fi/mautrix-meta/internal/media"
	"go.mau.fi/mautrix-meta/internal/projection"
	"go.mau.fi/mautrix-meta/internal/store"
)

type Server struct {
	Cache   *media.Cache
	DB      *store.Store
	Account string
	SDK     *sdk.Server
}
type Response struct {
	Media        []store.Media       `json:"media,omitempty"`
	Items        []projection.Result `json:"items,omitempty"`
	Status       []map[string]any    `json:"status,omitempty"`
	Next         string              `json:"next_cursor,omitempty"`
	Availability string              `json:"availability,omitempty"`
}

func New(db *store.Store, account string, cache *media.Cache) *Server {
	s := &Server{DB: db, Account: account, Cache: cache, SDK: sdk.NewServer(&sdk.Implementation{Name: "igdm-mirror", Version: "3"}, &sdk.ServerOptions{Instructions: "Read-only locally acquired Instagram source observations. Source content is reconstructed losslessly from normalized relational storage; subscription tokens are excluded from archived mailbox roots; source_hash identifies acquisition bytes, while normalized_content_hash identifies stored content. Source IDs resolve through account-scoped identities; entity_id and entity: references are stable local identities. source_hash identifies the acquisition root, which can contain the returned object as nested content. Use original identifier namespaces (id:, thread_fbid:, message_id:). Partial coverage and reconstruction uncertainty are explicit. No Instagram actions are available."})}
	tools := map[string]string{"list_accounts": "List locally known accounts and session metadata", "list_chats": "List conversation source snapshots", "get_chat": "Retrieve a conversation by an original namespaced source ID", "get_messages": "Retrieve conversation messages with bounded cursor pagination", "get_message": "Retrieve a message by an original namespaced source ID", "get_user": "Retrieve an identifiable user profile observation", "search_messages": "Search original message text paths with account, thread, sender and time filters", "get_media": "Retrieve locally cached media acquisition metadata", "get_sync_status": "Inspect synchronization coverage, progress, and recoverable gaps"}
	for name, description := range tools {
		tool := &sdk.Tool{Name: name, Description: description, OutputSchema: map[string]any{"type": "object", "properties": map[string]any{
			"media": map[string]any{"type": "array", "items": map[string]any{"type": "object"}}, "items": map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"status":      map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
			"next_cursor": map[string]any{"type": "string"}, "availability": map[string]any{"type": "string"},
		}}, Annotations: &sdk.ToolAnnotations{ReadOnlyHint: true}}
		if name == "list_accounts" {
			sdk.AddTool(s.SDK, tool, func(ctx context.Context, _ *sdk.CallToolRequest, _ struct{}) (*sdk.CallToolResult, Response, error) {
				out, err := s.call(ctx, name, store.Query{})
				return nil, out, err
			})
			continue
		}
		sdk.AddTool(s.SDK, tool, func(ctx context.Context, _ *sdk.CallToolRequest, q store.Query) (*sdk.CallToolResult, Response, error) {
			out, err := s.call(ctx, name, q)
			return nil, out, err
		})
	}
	for _, template := range []string{"igdm://accounts/{account_id}/entities/{kind}/{entity_id}", "igdm://accounts/{account_id}/threads/{thread_id}", "igdm://accounts/{account_id}/threads/{thread_id}/messages/{message_id}", "igdm://accounts/{account_id}/records/{kind}/{record_id}", "igdm://accounts/{account_id}/media/{hash}"} {
		s.SDK.AddResourceTemplate(&sdk.ResourceTemplate{Name: "Instagram source", URITemplate: template, MIMEType: "application/json"}, s.readResource)
	}
	return s
}
func (s *Server) authorize(account string) error {
	if account == "" || account != s.Account {
		return errors.New("account_id must match the account selected for this server")
	}
	return nil
}
func (s *Server) call(ctx context.Context, name string, q store.Query) (Response, error) {
	var out Response
	err := s.DB.ReadSnapshot(ctx, func(db *store.Store) error {
		local := *s
		local.DB = db
		var err error
		out, err = local.callSnapshot(ctx, name, q)
		return err
	})
	return out, err
}
func (s *Server) callSnapshot(ctx context.Context, name string, q store.Query) (Response, error) {
	out := Response{}
	if name == "list_accounts" {
		out.Status = []map[string]any{{"account_id": s.Account}}
		return out, nil
	}
	if err := s.authorize(q.Account); err != nil {
		return out, err
	}
	if name == "get_sync_status" {
		status, err := s.DB.Status(ctx, q.Account)
		out.Status = status
		return out, err
	}
	if name == "get_media" {
		return s.media(ctx, q)
	}
	table := "messages"
	switch name {
	case "list_chats", "get_chat":
		table = "threads"
	case "get_user":
		table = "users"
	}
	if name == "get_chat" || name == "get_message" || name == "get_user" {
		if q.SourceID == "" && q.EntityID == 0 {
			return out, errors.New("source_id or entity_id is required")
		}
		q.Limit = 2
	}
	if name == "get_messages" && q.ThreadID == "" {
		return out, errors.New("thread_id is required")
	}
	if name == "search_messages" && q.Text == "" {
		return out, errors.New("text is required")
	}
	page, err := s.DB.Query(ctx, table, q)
	if err != nil {
		return out, err
	}
	out.Next = page.Next
	if (name == "get_chat" || name == "get_message" || name == "get_user") && len(page.Records) > 1 {
		return out, errors.New("source ID is ambiguous; use an exact record resource")
	}
	out.Items = []projection.Result{}
	for _, r := range page.Records {
		result, err := projection.Build(ctx, s.DB, r, q.Raw)
		if err != nil {
			return out, errors.New("stored source projection failed")
		}
		out.Items = append(out.Items, result)
	}
	out.Availability = "partial"
	if len(page.Records) == 0 {
		out.Availability = "unavailable"
	}
	return out, nil
}
func (s *Server) media(ctx context.Context, q store.Query) (Response, error) {
	out := Response{Availability: "unavailable", Media: []store.Media{}}
	if q.SourceID == "" && q.EntityID == 0 {
		return out, errors.New("source_id or entity_id requires a message identity")
	}
	page, err := s.DB.Query(ctx, "messages", q)
	if err != nil {
		return out, err
	}
	for _, r := range page.Records {
		items, err := s.DB.Media(ctx, q.Account, r.RowID)
		if err != nil {
			return out, err
		}
		out.Media = append(out.Media, items...)
	}
	if len(out.Media) > 0 {
		out.Availability = "partial"
	}
	return out, nil
}
func (s *Server) readResource(ctx context.Context, req *sdk.ReadResourceRequest) (*sdk.ReadResourceResult, error) {
	var out *sdk.ReadResourceResult
	err := s.DB.ReadSnapshot(ctx, func(db *store.Store) error {
		local := *s
		local.DB = db
		if s.Cache != nil {
			cache := *s.Cache
			cache.DB = db
			local.Cache = &cache
		}
		var err error
		out, err = local.readResourceSnapshot(ctx, req)
		return err
	})
	return out, err
}
func (s *Server) readResourceSnapshot(ctx context.Context, req *sdk.ReadResourceRequest) (*sdk.ReadResourceResult, error) {
	uri := req.Params.URI
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "igdm" || u.Host != "accounts" {
		return nil, errors.New("invalid source resource")
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 3 {
		return nil, errors.New("invalid source resource")
	}
	if err = s.authorize(parts[0]); err != nil {
		return nil, err
	}
	var value any
	if parts[1] == "media" && len(parts) == 3 {
		if s.Cache == nil {
			return nil, errors.New("media cache unavailable")
		}
		offset := int64(0)
		length := media.ChunkSize
		if text := u.Query().Get("offset"); text != "" {
			offset, err = strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, errors.New("invalid media offset")
			}
		}
		if text := u.Query().Get("length"); text != "" {
			length, err = strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, errors.New("invalid media length")
			}
		}
		b, m, err := s.Cache.Read(ctx, s.Account, parts[2], offset, length)
		if err != nil {
			return nil, err
		}
		return &sdk.ReadResourceResult{Contents: []*sdk.ResourceContents{{URI: uri, MIMEType: m.MIME, Blob: b, Meta: sdk.Meta{"offset": offset, "byte_size": m.Size, "returned_bytes": len(b), "partial": offset != 0 || int64(len(b)) < m.Size}}}}, nil
	}
	if parts[1] == "records" && len(parts) == 4 {
		row, err := strconv.ParseInt(parts[3], 10, 64)
		if err != nil || row <= 0 {
			return nil, errors.New("invalid record")
		}
		r, err := s.DB.Record(ctx, s.Account, parts[2], row)
		if err != nil {
			return nil, errors.New("source record unavailable")
		}
		value = r
	} else if parts[1] == "entities" && len(parts) == 4 {
		id, parseErr := strconv.ParseInt(parts[3], 10, 64)
		if parseErr != nil || id <= 0 {
			return nil, errors.New("invalid entity reference")
		}
		name := map[string]string{"threads": "get_chat", "messages": "get_message", "users": "get_user"}[parts[2]]
		if name == "" {
			return nil, errors.New("invalid entity kind")
		}
		value, err = s.call(ctx, name, store.Query{Account: parts[0], EntityID: id, Raw: true})
		if err != nil {
			return nil, err
		}
	} else if parts[1] == "threads" && (len(parts) == 3 || len(parts) == 5 && parts[3] == "messages") {
		name := "get_chat"
		q := store.Query{Account: parts[0], SourceID: parts[2], Raw: true}
		if len(parts) == 5 {
			name = "get_message"
			q.SourceID = parts[4]
			q.ThreadID = parts[2]
		}
		value, err = s.call(ctx, name, q)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("invalid source resource")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return &sdk.ReadResourceResult{Contents: []*sdk.ResourceContents{{URI: uri, MIMEType: "application/json", Text: string(b)}}}, nil
}
func (s *Server) Run(ctx context.Context, address, tokenFile string, allowNetwork bool) error {
	if address == "" {
		return s.SDK.Run(ctx, &sdk.StdioTransport{})
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("HTTP address requires host:port: %w", err)
	}
	ip := net.ParseIP(host)
	loopback := host == "localhost" || ip != nil && ip.IsLoopback()
	if !loopback && !allowNetwork {
		return errors.New("non-loopback HTTP requires --allow-network")
	}
	info, err := os.Lstat(tokenFile)
	if err != nil {
		return errors.New("HTTP requires --http-token-file")
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 4096 {
		return errors.New("HTTP token file must be a small owner-only regular file")
	}
	token, err := os.ReadFile(tokenFile)
	if err != nil {
		return err
	}
	token = []byte(strings.TrimSpace(string(token)))
	if len(token) < 32 {
		return errors.New("HTTP token must contain at least 32 bytes")
	}
	expected := sha256.Sum256(token)
	clear(token)
	handler := sdk.NewStreamableHTTPHandler(func(*http.Request) *sdk.Server { return s.SDK }, nil)
	protected := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		presented := sha256.Sum256([]byte(strings.TrimPrefix(auth, "Bearer ")))
		if !strings.HasPrefix(auth, "Bearer ") || subtle.ConstantTimeCompare(expected[:], presented[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host && origin != "https://"+r.Host {
			http.Error(w, "origin denied", http.StatusForbidden)
			return
		}
		if r.URL.Path != "/mcp" {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	})
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: protected, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16384}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err = <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = server.Close()
		return ctx.Err()
	}
}
