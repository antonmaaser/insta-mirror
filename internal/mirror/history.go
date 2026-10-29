package mirror

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
)

type captureKey struct{}
type captureFunc func(context.Context, string, []byte) error

func captureGraphQL(ctx context.Context, operation string, raw []byte) error {
	if fn, ok := ctx.Value(captureKey{}).(captureFunc); ok {
		return fn(ctx, operation, raw)
	}
	return nil
}

type Engine struct {
	DB             *store.Store
	Client         *instameow.Client
	Account        string
	Abort          context.CancelCauseFunc
	Reconcile      chan struct{}
	SaveConnection func(json.RawMessage) error
}
type pageSpec struct {
	Scope, ID, Path, Previous string
	Preserve                  bool
	Messages                  bool
}
type rawPage struct {
	Edges []struct {
		Node json.RawMessage `json:"node"`
	} `json:"edges"`
	Info *slidetypes.PageInfo `json:"page_info"`
}

func field(raw json.RawMessage, path ...string) json.RawMessage {
	for _, key := range path {
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) != nil {
			return nil
		}
		raw = obj[key]
	}
	return raw
}
func object(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }
func (e *Engine) archive(ctx context.Context, raw []byte, spec pageSpec) error {
	var envelope struct {
		Errors    json.RawMessage `json:"errors"`
		ErrorCode int             `json:"error"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return errors.New("invalid GraphQL JSON")
	}
	if envelope.ErrorCode != 0 || (object(envelope.Errors) && string(envelope.Errors) != "[]") {
		return nil
	}
	source := field(raw, "data", spec.Path)
	if !object(source) {
		return errors.New("source page unavailable; coverage is incomplete")
	}
	container := source
	if spec.Messages {
		container = field(source, "as_ig_direct_thread")
	}
	if !object(container) {
		return errors.New("thread is inaccessible; existing history retained")
	}
	pageField := "threads_by_folder"
	if spec.Messages {
		pageField = "slide_messages"
	}
	var page rawPage
	if err := json.Unmarshal(field(container, pageField), &page); err != nil || page.Info == nil {
		return errors.New("source did not provide pagination coverage")
	}
	now := time.Now().UnixMilli()
	state := store.State{Cursor: page.Info.EndCursor, Complete: !page.Info.HasNextPage, LastSuccess: now}
	stalled := !state.Complete && (state.Cursor == "" || state.Cursor == spec.Previous)
	if stalled {
		state.Error = "pagination cursor did not advance"
		state.Gaps = []string{state.Error}
	}
	err := e.DB.Transaction(ctx, func(tx *sql.Tx) error {
		if spec.Messages {
			if store.Identity("threads", container) != "" && store.String(container, "thread_fbid") != "" {
				if err := e.thread(ctx, tx, container, spec.Path, now); err != nil {
					return err
				}
			}
			for _, edge := range page.Edges {
				if err := e.message(ctx, tx, edge.Node, spec.ID, spec.Path, now); err != nil {
					return err
				}
			}
		} else {
			for _, edge := range page.Edges {
				t := field(edge.Node, "as_ig_direct_thread")
				if !object(t) {
					continue
				}
				if err := e.thread(ctx, tx, t, spec.Path, now); err != nil {
					return err
				}
			}
			var pinned []json.RawMessage
			if err := json.Unmarshal(field(container, "pinned_threads_v2"), &pinned); err == nil {
				for _, p := range pinned {
					t := field(p, "as_ig_direct_thread")
					if object(t) {
						if err := e.thread(ctx, tx, t, spec.Path, now); err != nil {
							return err
						}
					}
				}
			}
		}
		if spec.Path == "get_slide_mailbox_for_iris_subscription" {
			var checkpoint store.State
			// Read through the transaction; Store.DB has a single connection.
			var prior string
			err := tx.QueryRowContext(ctx, `SELECT state FROM sync_state WHERE account_id=? AND scope='realtime' AND scope_id='checkpoint'`, e.Account).Scan(&prior)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if prior != "" {
				if err = json.Unmarshal([]byte(prior), &checkpoint); err != nil {
					return err
				}
			}
			if checkpoint.SeqID == 0 {
				seq, err := strconv.ParseInt(store.String(source, "iris_inactive_subscription_uq_seq_id"), 10, 64)
				if err != nil || seq <= 0 {
					return errors.New("mailbox has no valid subscription sequence")
				}
				if err = store.SaveState(ctx, tx, e.Account, "realtime", "checkpoint", store.State{SeqID: seq, Timestamp: now, LastSuccess: now, Gaps: []string{"initial snapshot baseline; older realtime events were not acquired"}}); err != nil {
					return err
				}
			}
		}
		if spec.Preserve {
			return nil
		}
		return store.SaveState(ctx, tx, e.Account, spec.Scope, spec.ID, state)
	})
	if err != nil {
		return err
	}
	if stalled && !spec.Preserve {
		return errors.New(state.Error)
	}
	return nil
}
func (e *Engine) user(ctx context.Context, tx *sql.Tx, raw json.RawMessage, origin string, now int64) error {
	if !object(raw) || store.Identity("users", raw) == "" {
		return nil
	}
	// Embedded partial profiles remain observations, never replacements for fuller objects.
	return store.Put(ctx, tx, store.Record{Table: "users", Account: e.Account, Raw: raw, Origin: origin, Observed: now})
}
func (e *Engine) thread(ctx context.Context, tx *sql.Tx, raw json.RawMessage, origin string, now int64) error {
	if err := store.Put(ctx, tx, store.Record{Table: "threads", Account: e.Account, Raw: raw, Origin: origin, Observed: now}); err != nil {
		return err
	}
	var users []json.RawMessage
	if err := json.Unmarshal(field(raw, "users"), &users); err == nil {
		for _, u := range users {
			if err = e.user(ctx, tx, u, origin, now); err != nil {
				return err
			}
		}
	}
	return e.user(ctx, tx, field(raw, "viewer"), origin, now)
}
func (e *Engine) message(ctx context.Context, tx *sql.Tx, raw json.RawMessage, thread, origin string, now int64) error {
	if !object(raw) {
		return nil
	}
	// Inbox snippets are never passed here; only complete history or delta message nodes.
	if err := store.Put(ctx, tx, store.Record{Table: "messages", Account: e.Account, Thread: thread, Raw: raw, Origin: origin, Observed: now}); err != nil {
		return err
	}
	return e.user(ctx, tx, field(raw, "sender", "user_dict"), origin, now)
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (e *Engine) fetch(ctx context.Context, spec pageSpec, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if err = pause(ctx, 800*time.Millisecond+time.Duration(rand.IntN(400))*time.Millisecond); err != nil {
			return err
		}
		captured := false
		callCtx := context.WithValue(ctx, captureKey{}, captureFunc(func(ctx context.Context, _ string, b []byte) error { captured = true; return e.archive(ctx, b, spec) }))
		err = fn(callCtx)
		if err == nil && !captured {
			err = errors.New("source response was not captured")
		}
		if err == nil {
			return nil
		}
		var network net.Error
		if !errors.As(err, &network) && !errors.Is(err, httpclient.ErrRateLimited) {
			break
		}
		if err = pause(ctx, time.Duration(1<<attempt)*time.Second+time.Duration(rand.IntN(1000))*time.Millisecond); err != nil {
			return err
		}
	}
	var state store.State
	if stateErr := e.DB.State(ctx, e.Account, spec.Scope, spec.ID, &state); stateErr != nil {
		return stateErr
	}
	state.Error = "source retrieval or archival failed; retry synchronization"
	if errors.Is(err, httpclient.ErrChallengeRequired) || errors.Is(err, httpclient.ErrCheckpointRequired) || errors.Is(err, httpclient.ErrTokenInvalidated) {
		state.Error = "authentication requires attention; run login"
	}
	if saveErr := e.DB.SetState(ctx, e.Account, spec.Scope, spec.ID, state); saveErr != nil {
		return saveErr
	}
	return fmt.Errorf("%s %s: %s", spec.Scope, spec.ID, state.Error)
}
func (e *Engine) Initialize(ctx context.Context) error {
	var saved store.State
	if err := e.DB.State(ctx, e.Account, "folder", "INBOX", &saved); err != nil {
		return err
	}
	return e.fetch(ctx, pageSpec{Scope: "folder", ID: "INBOX", Path: "get_slide_mailbox_for_iris_subscription", Preserve: !saved.Complete && saved.Cursor != ""}, func(ctx context.Context) error { _, _, err := e.Client.LoadIndex(ctx); return err })
}
func (e *Engine) History(ctx context.Context) error {
	// PENDING and SPAM are upstream source folder identifiers; each request must
	// return an actual page before its coverage can be claimed.
	var folderErrors []string
	for _, folder := range []string{"INBOX", "PENDING", "SPAM"} {
		var state store.State
		if err := e.DB.State(ctx, e.Account, "folder", folder, &state); err != nil {
			return err
		}
		if folder != "INBOX" && state.Complete {
			state = store.State{}
		}
		for !state.Complete {
			cursor := state.Cursor
			spec := pageSpec{Scope: "folder", ID: folder, Path: "fetch__SlideMailbox", Previous: cursor}
			err := e.fetch(ctx, spec, func(ctx context.Context) error {
				_, err := e.Client.PaginateMailbox(ctx, slidetypes.MakePaginateMailboxRequest(e.Client.GetOwnFBID(), cursor, folder, nil))
				return err
			})
			if err != nil {
				if folder == "INBOX" {
					return err
				}
				folderErrors = append(folderErrors, folder+": unsupported or inaccessible; see folder state")
				break
			}
			if err = e.DB.State(ctx, e.Account, "folder", folder, &state); err != nil {
				return err
			}
		}
	}
	rows, err := e.DB.DB.QueryContext(ctx, `SELECT DISTINCT thread_fbid FROM threads WHERE account_id=? AND thread_fbid IS NOT NULL AND thread_fbid!=''`, e.Account)
	if err != nil {
		return err
	}
	var threads []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		threads = append(threads, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures int
	for _, id := range threads {
		if err = e.historyThread(ctx, id); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			failures++
		}
	}
	state := store.State{Complete: failures == 0, LastSuccess: time.Now().UnixMilli(), Gaps: folderErrors}
	if failures > 0 {
		state.Error = fmt.Sprintf("%d incomplete thread histories", failures)
	}
	if err = e.DB.SetState(ctx, e.Account, "reconciliation", "history", state); err != nil {
		return err
	}
	if failures > 0 {
		return errors.New(state.Error)
	}
	return nil
}
func (e *Engine) historyThread(ctx context.Context, id string) error {
	var saved store.State
	if err := e.DB.State(ctx, e.Account, "thread", id, &saved); err != nil {
		return err
	}
	preserve := saved.Cursor != "" && !saved.Complete
	spec := pageSpec{Scope: "thread", ID: id, Path: "get_slide_thread_nullable", Messages: true, Preserve: preserve}
	if err := e.fetch(ctx, spec, func(ctx context.Context) error {
		_, err := e.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(id))
		return err
	}); err != nil {
		return err
	}
	var paginationID string
	if err := e.DB.DB.QueryRowContext(ctx, `SELECT id FROM threads WHERE account_id=? AND thread_fbid=? AND id IS NOT NULL AND id!='' ORDER BY last_observed_at_ms DESC,record_id DESC LIMIT 1`, e.Account, id).Scan(&paginationID); err != nil {
		return fmt.Errorf("missing source pagination identity: %w", err)
	}
	for {
		var state store.State
		if err := e.DB.State(ctx, e.Account, "thread", id, &state); err != nil {
			return err
		}
		if state.Complete {
			return nil
		}
		cursor := state.Cursor
		spec = pageSpec{Scope: "thread", ID: id, Path: "fetch__SlideThread", Messages: true, Previous: cursor}
		if err := e.fetch(ctx, spec, func(ctx context.Context) error {
			_, err := e.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{AfterCursor: &cursor, ThreadID: paginationID, FirstN: 20, InitialMessagePageCount: 20})
			return err
		}); err != nil {
			return err
		}
	}
}
