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
	Scope, ID, Path, Previous  string
	Start                      bool
	Revision, RealtimeRevision int64
	Remote                     string
	Preserve                   bool
	Messages                   bool
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
func (e *Engine) applyPage(ctx context.Context, raw []byte, spec pageSpec) error {
	envelope, err := decodeObject(raw)
	if err != nil {
		return err
	}
	if envelope.id("error") != "" || envelope.id("error_code") != "" || object(envelope["errors"]) && string(envelope["errors"]) != "[]" {
		return errors.New("partial GraphQL response")
	}
	source := field(raw, "data", spec.Path)
	container := source
	pageField := "threads_by_folder"
	if spec.Messages {
		container = field(source, "as_ig_direct_thread")
		pageField = "slide_messages"
	}
	if !object(container) {
		return errors.New("source unavailable; previous coverage retained")
	}
	pagination := field(container, pageField)
	var page rawPage
	if json.Unmarshal(pagination, &page) != nil || page.Info == nil || !object(field(pagination, "edges")) {
		return errors.New("source page has no valid pagination coverage")
	}
	switch string(field(pagination, "page_info", "has_next_page")) {
	case "true", "false":
	default:
		return errors.New("source page has no explicit pagination boundary")
	}
	return e.ingestTransaction(ctx, spec.Scope+":"+spec.ID, func(b *ingestion) error {
		tx := b.tx
		var prior, realtime store.State
		if err := store.ReadState(ctx, tx, e.Account, spec.Scope, spec.ID, &prior); err != nil {
			return err
		}
		if prior.Revision != spec.Revision {
			return errors.New("stale acquisition checkpoint")
		}
		if err := store.ReadState(ctx, tx, e.Account, "realtime", "checkpoint", &realtime); err != nil {
			return err
		}
		b.fill = spec.Scope == "thread" || spec.Messages && !spec.Start || realtime.Revision != spec.RealtimeRevision
		now := time.Now().UnixMilli()
		state := store.State{Cursor: page.Info.EndCursor, Complete: !page.Info.HasNextPage, LastSuccess: now, Revision: prior.Revision + 1, Generation: prior.Generation}
		var ids []int64
		if spec.Messages {
			local, err := strconv.ParseInt(spec.ID, 10, 64)
			if err != nil {
				return err
			}
			thread, err := b.thread(container, local, spec.Remote, spec.Path == "get_slide_thread_nullable")
			if err != nil {
				return err
			}
			var deleted sql.NullBool
			if err = tx.QueryRowContext(ctx, "SELECT deleted FROM threads WHERE id=?", thread).Scan(&deleted); err != nil {
				return err
			}
			if deleted.Bool {
				return nil
			}
			for _, edge := range page.Edges {
				id, err := b.message(edge.Node, thread, false)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
		} else {
			for _, edge := range page.Edges {
				id, err := b.inboxThread(edge.Node, false)
				if err != nil {
					return err
				}
				ids = append(ids, id)
			}
			if rawPinned := field(source, "pinned_threads_v2"); object(rawPinned) {
				var pinned []json.RawMessage
				if json.Unmarshal(rawPinned, &pinned) != nil {
					return errors.New("invalid pinned threads")
				}
				for _, item := range pinned {
					if _, err := b.inboxThread(item, true); err != nil {
						return err
					}
				}
			}
		}
		if spec.Scope == "head" {
			state.Anchor, state.Target, state.Candidate = prior.Anchor, prior.Target, prior.Candidate
			if spec.Start {
				state.Target, state.Candidate = prior.Anchor, ""
			}
			if state.Candidate == "" && len(ids) > 0 {
				state.Candidate = strconv.FormatInt(ids[0], 10)
			}
			if state.Target == "" {
				state.Complete = true
			}
			for _, id := range ids {
				if strconv.FormatInt(id, 10) == state.Target {
					state.Complete = true
				}
			}
			if state.Complete {
				if state.Candidate != "" {
					state.Anchor = state.Candidate
				}
				state.Target, state.Candidate = "", ""
			}
		}
		state.PageHash = store.Hash([]byte(store.JSONValue(ids)))
		if !spec.Preserve && !state.Complete && (state.Cursor == "" || state.Cursor == spec.Previous || prior.PageHash == state.PageHash) {
			return errors.New("pagination did not advance")
		}
		if spec.Path == "get_slide_mailbox_for_iris_subscription" {
			var resnapshot store.State
			if err := store.ReadState(ctx, tx, e.Account, "realtime", "resnapshot", &resnapshot); err != nil {
				return err
			}
			if realtime.SeqID == 0 || len(resnapshot.Gaps) > 0 {
				o, err := decodeObject(source)
				if err != nil {
					return err
				}
				seq, err := strconv.ParseInt(o.id("iris_inactive_subscription_uq_seq_id"), 10, 64)
				if err != nil || seq <= 0 {
					return errors.New("mailbox has no valid subscription sequence")
				}
				realtime.SeqID, realtime.Timestamp, realtime.LastSuccess = seq, now, now
				realtime.Revision++
				realtime.PageHash, realtime.Error = "", ""
				realtime.Gaps = resnapshot.Gaps
				if len(realtime.Gaps) == 0 {
					realtime.Gaps = []string{"initial mailbox baseline; earlier realtime changes were not acquired"}
				}
				if err = store.SaveState(ctx, tx, e.Account, "realtime", "checkpoint", realtime); err != nil {
					return err
				}
			}
		}
		if spec.Scope == "head" && spec.Start {
			var tail store.State
			if err := store.ReadState(ctx, tx, e.Account, "thread", spec.ID, &tail); err != nil {
				return err
			}
			if tail.LastSuccess == 0 {
				tail.Cursor, tail.Complete, tail.LastSuccess, tail.PageHash = page.Info.EndCursor, !page.Info.HasNextPage, now, state.PageHash
				tail.Revision++
				if !tail.Complete && tail.Cursor == "" {
					return errors.New("initial thread page has no continuation cursor")
				}
				if err := store.SaveState(ctx, tx, e.Account, "thread", spec.ID, tail); err != nil {
					return err
				}
			}
		}
		if spec.Preserve {
			return nil
		}
		return store.SaveState(ctx, tx, e.Account, spec.Scope, spec.ID, state)
	})
}

func (b *ingestion) inboxThread(raw json.RawMessage, pinned bool) (int64, error) {
	wrapper, err := decodeObject(raw)
	if err != nil {
		return 0, err
	}
	thread, err := b.thread(wrapper["as_ig_direct_thread"], 0, wrapper.id("id"), false)
	if err != nil {
		return 0, err
	}
	if pinned {
		fields, err := b.patch("threads", thread, map[string]any{"pinned": int64(1)}, b.fill)
		if err != nil {
			return 0, err
		}
		b.decision("threads", thread, false, false, b.fill, fields)
	}
	prior := b.fill
	b.fill = true
	defer func() { b.fill = prior }()
	if rawMessages := field(raw, "as_ig_direct_thread", "slide_messages", "edges"); object(rawMessages) {
		var edges []struct {
			Node json.RawMessage `json:"node"`
		}
		if json.Unmarshal(rawMessages, &edges) != nil {
			return 0, errors.New("invalid inbox messages")
		}
		for _, edge := range edges {
			if _, err = b.message(edge.Node, thread, false); err != nil {
				return 0, err
			}
		}
	}
	return thread, nil
}

type acquisitionError struct{ error }

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
	var realtime store.State
	if err := e.DB.State(ctx, e.Account, "realtime", "checkpoint", &realtime); err != nil {
		return err
	}
	spec.RealtimeRevision = realtime.Revision
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
			if err := e.applyPage(ctx, b, spec); err != nil {
				return acquisitionError{err}
			}
			return nil
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
	state.Error = "source retrieval failed; retry synchronization"
	var blocked acquisitionError
	if errors.As(err, &blocked) {
		state.Error = blocked.Error()
	}
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
		if len(state.Gaps) > 0 {
			folderErrors = append(folderErrors, folder+": source availability boundaries retained; see folder state")
		}
	}
	rows, err := e.DB.DB.QueryContext(ctx, `SELECT t.id, i.remote_id FROM threads t JOIN source_ids i ON i.account_id=t.account_id AND i.thread_id=t.id AND i.namespace='thread.fbid' WHERE t.account_id=? AND coalesce(t.deleted,0)=0 ORDER BY t.id`, e.Account)
	if err != nil {
		return err
	}
	type target struct {
		local  int64
		remote string
	}
	var threads []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.local, &t.remote); err != nil {
			rows.Close()
			return err
		}
		threads = append(threads, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures int
	for _, target := range threads {
		if err = e.historyThread(ctx, strconv.FormatInt(target.local, 10), target.remote); err != nil {
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
	if err = e.DB.DB.QueryRowContext(ctx, `SELECT count(*) FROM threads t WHERE t.account_id=? AND coalesce(t.deleted,0)=0 AND NOT EXISTS(SELECT 1 FROM source_ids i WHERE i.account_id=t.account_id AND i.thread_id=t.id AND i.namespace='thread.fbid')`, e.Account).Scan(&unresolved); err != nil {
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
func (e *Engine) historyThread(ctx context.Context, id, remote string) error {
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
		if err := e.fetch(ctx, pageSpec{Scope: "head", ID: id, Remote: remote, Path: "get_slide_thread_nullable", Messages: true, Start: true}, func(ctx context.Context) error {
			_, err := e.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(remote))
			return err
		}); err != nil {
			return err
		}
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
			spec := pageSpec{Scope: scope, ID: id, Remote: remote, Path: "fetch__SlideThread", Messages: true, Previous: cursor}
			if err = e.fetch(ctx, spec, func(ctx context.Context) error {
				_, err := e.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{AfterCursor: &cursor, ThreadID: remote, FirstN: 20, InitialMessagePageCount: 20})
				return err
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
