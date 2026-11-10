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
	Log            *syncLog
	Client         *instameow.Client
	Account        string
	Abort          context.CancelCauseFunc
	Reconcile      chan struct{}
	SaveConnection func(json.RawMessage) error
}
type pageSpec struct {
	Scope, ID, Path, Previous string
	Start                     bool
	Revision                  int64
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
	flag := string(field(container, pageField, "page_info", "has_next_page"))
	if flag != "true" && flag != "false" {
		return errors.New("source did not provide an explicit pagination boundary")
	}
	now := time.Now().UnixMilli()
	pageBytes := []byte{}
	for _, edge := range page.Edges {
		pageBytes = append(pageBytes, edge.Node...)
		pageBytes = append(pageBytes, '\n')
	}
	var stalled, stale bool
	err := e.archiveTransaction(ctx, func(tx *sql.Tx) error {
		var prior store.State
		if err := store.ReadState(ctx, tx, e.Account, spec.Scope, spec.ID, &prior); err != nil {
			return err
		}
		stale = prior.Revision != spec.Revision
		state := store.State{Cursor: page.Info.EndCursor, Complete: !page.Info.HasNextPage, LastSuccess: now, PageHash: store.Hash(pageBytes), Revision: prior.Revision + 1, Generation: prior.Generation}
		if spec.Scope == "head" {
			state.Anchor, state.Target, state.Candidate = prior.Anchor, prior.Target, prior.Candidate
			if spec.Start {
				state.Target = prior.Anchor
				state.Candidate = ""
			}
			if state.Candidate == "" && len(page.Edges) > 0 {
				state.Candidate = store.String(page.Edges[0].Node, "id")
			}
			// Only the prior completed full-message head is an overlap boundary.
			// An initial head seeds backfill and need not duplicate that entire walk.
			if state.Target == "" {
				state.Complete = true
			}
			for _, edge := range page.Edges {
				if state.Target != "" && store.String(edge.Node, "id") == state.Target {
					state.Complete = true
				}
			}
			if state.Complete {
				state.Anchor = state.Candidate
				state.Target = ""
				state.Candidate = ""
			}
		}
		stalled = !state.Complete && (state.Cursor == "" || state.Cursor == spec.Previous || prior.PageHash == state.PageHash && prior.Cursor != state.Cursor)
		if stalled {
			state.Cursor = prior.Cursor
			state.Error = "pagination cursor did not advance"
			state.Gaps = []string{state.Error}
		}

		if spec.Messages {
			if err := store.Put(ctx, tx, store.Record{Table: "threads", Account: e.Account, Raw: container, Thread: spec.ID, Origin: spec.Path, Context: spec.Scope + ":" + spec.ID + ":" + spec.Previous, Observed: now}); err != nil {
				return err
			}
		} else {
			for _, edge := range page.Edges {
				t := field(edge.Node, "as_ig_direct_thread")
				if !object(t) {
					continue
				}
				if err := e.thread(ctx, tx, t, spec.Path, spec.Scope+":"+spec.ID, now); err != nil {
					return err
				}
			}
			var pinned []json.RawMessage
			if err := json.Unmarshal(field(container, "pinned_threads_v2"), &pinned); err == nil {
				for _, p := range pinned {
					t := field(p, "as_ig_direct_thread")
					if object(t) {
						if err := e.thread(ctx, tx, t, spec.Path, spec.Scope+":"+spec.ID, now); err != nil {
							return err
						}
					}
				}
			}
		}
		if spec.Path == "get_slide_mailbox_for_iris_subscription" {
			var checkpoint, resnapshot store.State
			if err := store.ReadState(ctx, tx, e.Account, "realtime", "checkpoint", &checkpoint); err != nil {
				return err
			}
			if err := store.ReadState(ctx, tx, e.Account, "realtime", "resnapshot", &resnapshot); err != nil {
				return err
			}
			if checkpoint.SeqID == 0 || len(resnapshot.Gaps) > 0 {
				seq, err := strconv.ParseInt(store.String(source, "iris_inactive_subscription_uq_seq_id"), 10, 64)
				if err != nil || seq <= 0 {
					return errors.New("mailbox has no valid subscription sequence")
				}
				gaps := []string{"initial snapshot baseline; older realtime events were not acquired"}
				if len(resnapshot.Gaps) > 0 {
					gaps = resnapshot.Gaps
				}
				if err = store.SaveState(ctx, tx, e.Account, "realtime", "checkpoint", store.State{SeqID: seq, Timestamp: now, LastSuccess: now, Gaps: gaps}); err != nil {
					return err
				}
			}
		}
		if spec.Scope == "head" && !stale {
			var tail store.State
			if err := store.ReadState(ctx, tx, e.Account, "thread", spec.ID, &tail); err != nil {
				return err
			}
			if tail.LastSuccess == 0 {
				tail.Cursor = page.Info.EndCursor
				tail.Complete = !page.Info.HasNextPage
				tail.LastSuccess = now
				tail.PageHash = store.Hash(pageBytes)
				tail.Revision++
				if err := store.SaveState(ctx, tx, e.Account, "thread", spec.ID, tail); err != nil {
					return err
				}
			}
		}

		if spec.Preserve || stale {
			return nil
		}
		return store.SaveState(ctx, tx, e.Account, spec.Scope, spec.ID, state)
	})
	if err != nil {
		return err
	}
	if stale && !spec.Preserve {
		return errors.New("stale page retained without changing its checkpoint")
	}
	if stalled && !spec.Preserve {
		return errors.New("pagination cursor did not advance")
	}
	return nil
}
func (e *Engine) thread(ctx context.Context, tx *sql.Tx, raw json.RawMessage, origin, contextID string, now int64) error {
	return store.Put(ctx, tx, store.Record{Table: "threads", Account: e.Account, Raw: raw, Origin: origin, Context: contextID, Observed: now})
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
	var expected store.State
	if err := e.DB.State(ctx, e.Account, spec.Scope, spec.ID, &expected); err != nil {
		return err
	}
	spec.Revision = expected.Revision
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if err = pause(ctx, 800*time.Millisecond+time.Duration(rand.IntN(400))*time.Millisecond); err != nil {
			return err
		}
		captured := false
		callCtx := context.WithValue(ctx, captureKey{}, captureFunc(func(ctx context.Context, operation string, b []byte) error {
			expected := map[string]string{"get_slide_mailbox_for_iris_subscription": "PolarisDirectInboxQuery", "fetch__SlideMailbox": "IGDThreadListOffMsysPaginationQuery", "get_slide_thread_nullable": "IGDThreadDetailQuery", "fetch__SlideThread": "IGDMessageListOffMsysQuery"}[spec.Path]
			if operation != expected {
				return nil
			}
			captured = true
			return e.archive(ctx, b, spec)
		}))
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
	return e.fetch(ctx, pageSpec{Scope: "folder", ID: "INBOX", Path: "get_slide_mailbox_for_iris_subscription", Preserve: !saved.Complete && saved.Cursor != "", Start: true}, func(ctx context.Context) error { _, _, err := e.Client.LoadIndex(ctx); return err })
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
		if state.Complete {
			state = store.State{Revision: state.Revision, Generation: state.Generation}
			if err := e.DB.SetState(ctx, e.Account, "folder", folder, state); err != nil {
				return err
			}
		}
		seen := map[string]bool{}
		for !state.Complete {
			cursor := state.Cursor
			if seen[cursor] {
				state.Error = "folder pagination cycle detected"
				if err := e.DB.SetState(ctx, e.Account, "folder", folder, state); err != nil {
					return err
				}
				return errors.New(state.Error)
			}
			seen[cursor] = true
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
	rows, err := e.DB.DB.QueryContext(ctx, `SELECT DISTINCT value FROM source_ids WHERE account_id=? AND namespace='thread.fbid'`, e.Account)
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
	var previous store.State
	if err = e.DB.State(ctx, e.Account, "reconciliation", "history", &previous); err != nil {
		return err
	}
	state := store.State{Complete: failures == 0 && len(folderErrors) == 0, LastSuccess: previous.LastSuccess, Gaps: folderErrors}
	var unresolved int
	if err = e.DB.DB.QueryRowContext(ctx, `SELECT count(*) FROM threads t WHERE t.account_id=? AND t.merged_into IS NULL AND NOT EXISTS(SELECT 1 FROM source_ids i WHERE i.account_id=t.account_id AND i.thread_entity_id=t.entity_id AND i.namespace='thread.fbid')`, e.Account).Scan(&unresolved); err != nil {
		return err
	}
	if unresolved > 0 {
		state.Complete = false
		state.Gaps = append(state.Gaps, fmt.Sprintf("%d source threads lack history acquisition identities", unresolved))
	}
	if state.Complete {
		state.LastSuccess = time.Now().UnixMilli()
	}
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
	var gap store.State
	if err := e.DB.State(ctx, e.Account, "reconciliation", "resnapshot", &gap); err != nil {
		return err
	}
	for _, scope := range []string{"head", "thread"} {
		var state store.State
		if err := e.DB.State(ctx, e.Account, scope, id, &state); err != nil {
			return err
		}
		if state.Generation < gap.Timestamp {
			state = store.State{Generation: gap.Timestamp, Revision: state.Revision + 1}
			if err := e.DB.SetState(ctx, e.Account, scope, id, state); err != nil {
				return err
			}
		}
	}
	var head store.State
	if err := e.DB.State(ctx, e.Account, "head", id, &head); err != nil {
		return err
	}
	if head.Complete || head.LastSuccess == 0 {
		if err := e.fetch(ctx, pageSpec{Scope: "head", ID: id, Path: "get_slide_thread_nullable", Messages: true, Start: true}, func(ctx context.Context) error {
			_, err := e.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(id))
			return err
		}); err != nil {
			return err
		}
	}
	// Upstream GetThread uses the short FBID; pagination uses the returned id.
	entity, err := e.DB.ResolveEntity(ctx, e.Account, "threads", "thread_fbid:"+id)
	if err != nil {
		return err
	}
	var paginationID string
	if err = e.DB.DB.QueryRowContext(ctx, `SELECT value FROM source_ids WHERE account_id=? AND thread_entity_id=? AND namespace='thread.fbid' ORDER BY source_id LIMIT 1`, e.Account, entity).Scan(&paginationID); err != nil {
		return err
	}
	for _, scope := range []string{"head", "thread"} {
		seen := map[string]bool{}
		for {
			var state store.State
			if err = e.DB.State(ctx, e.Account, scope, id, &state); err != nil {
				return err
			}
			if state.Complete {
				break
			}
			cursor := state.Cursor
			if cursor == "" || seen[cursor] {
				state.Error = "message pagination has no advancing cursor"
				if err = e.DB.SetState(ctx, e.Account, scope, id, state); err != nil {
					return err
				}
				return errors.New(state.Error)
			}
			seen[cursor] = true
			spec := pageSpec{Scope: scope, ID: id, Path: "fetch__SlideThread", Messages: true, Previous: cursor}
			if err = e.fetch(ctx, spec, func(ctx context.Context) error {
				_, err := e.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{AfterCursor: &cursor, ThreadID: paginationID, FirstN: 20, InitialMessagePageCount: 20})
				return err
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
