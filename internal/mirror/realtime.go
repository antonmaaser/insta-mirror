package mirror

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func (e *Engine) captureDeltas(ctx context.Context, b []byte) error {
	var wrappers []struct {
		Data struct {
			Deltas []json.RawMessage `json:"slide_delta_processor"`
		} `json:"data"`
	}
	var rawWrappers []json.RawMessage
	malformed := json.Unmarshal(b, &wrappers) != nil || json.Unmarshal(b, &rawWrappers) != nil
	if !malformed {
		for _, rawWrapper := range rawWrappers {
			if !object(field(rawWrapper, "data", "slide_delta_processor")) {
				malformed = true
				break
			}
		}
		for _, wrapper := range wrappers {
			for _, raw := range wrapper.Data.Deltas {
				var obj map[string]json.RawMessage
				if json.Unmarshal(raw, &obj) != nil || obj == nil {
					malformed = true
					break
				}
			}
		}
	}
	if malformed {
		return e.unparsedBatch(ctx, b)
	}
	var parseFailure bool
	err := e.archiveTransaction(ctx, func(tx *sql.Tx) error {
		now := time.Now().UnixMilli()
		for wi, w := range wrappers {
			for di, raw := range w.Data.Deltas {
				identity := store.Identity("deltas", raw)
				if identity == "" {
					// Without source sequence, retain each arrival as an observation. The hash
					// remains deterministic, while the local occurrence prevents false merges.
					identity = fmt.Sprintf("unsequenced:%s:%d:%d:%d", store.Hash(raw), time.Now().UnixNano(), wi, di)
					if err := store.SaveState(ctx, tx, e.Account, "realtime", "unsequenced", store.State{Gaps: []string{"events without source sequence cannot be reliably deduplicated"}}); err != nil {
						return err
					}
				}
				if err := store.Put(ctx, tx, store.Record{Table: "deltas", Account: e.Account, Identity: identity, Origin: "dgw", Raw: raw, Observed: now}); err != nil {
					return err
				}
				var delta slidetypes.Delta
				decodeErr := json.Unmarshal(raw, &delta)
				_, unknown := delta.Data.(slidetypes.UnknownEvent)
				if decodeErr != nil || unknown {
					label := "unknown delta retained for query-time interpretation"
					if decodeErr != nil {
						label = "delta decoding failed; original source retained"
						parseFailure = true
					}
					if err := store.SaveState(ctx, tx, e.Account, "realtime", "delta:"+identity, store.State{Error: label, SeqID: delta.UQSeqID}); err != nil {
						return err
					}
				}
				msg := field(raw, "message")
				if object(msg) && store.Identity("messages", msg) != "" {
					// All nested source bytes are retained even if their typed decoding fails.
					if err := e.message(ctx, tx, msg, store.String(raw, "thread_fbid"), fmt.Sprintf("dgw:%d", delta.UQSeqID), now); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		e.fail(err)
		return err
	}
	if parseFailure {
		err = errors.New("realtime delta decoding failed; checkpoint retained for recovery")
		e.fail(err)
		return err
	}
	return nil
}
func (e *Engine) fail(err error) {
	if e.Abort != nil {
		e.Abort(err)
	}
}
func (e *Engine) requestReconcile() {
	if e.Reconcile != nil {
		select {
		case e.Reconcile <- struct{}{}:
		default:
		}
	}
}
func (e *Engine) event(ctx context.Context, event slidetypes.ClientEvent) error {
	var err error
	switch v := event.(type) {
	case *slidetypes.Delta:
		// The raw batch callback has already archived every delta transactionally.
		return nil
	case *slidetypes.SeqIDUpdate:
		err = e.DB.SetState(ctx, e.Account, "realtime", "checkpoint", store.State{SeqID: v.SeqID, Timestamp: v.Timestamp.UnixMilli(), LastSuccess: time.Now().UnixMilli()})
	case *slidetypes.ReconnectionStateUpdate:
		if e.SaveConnection != nil {
			err = e.SaveConnection(v.State)
		}
	case *slidetypes.Connected:
		err = e.DB.SetState(ctx, e.Account, "realtime", "connection", map[string]any{"connected": true, "observed_at_ms": time.Now().UnixMilli(), "subscribed_seq_id": v.SubscribedSeqID, "latest_seq_id": v.LatestSeqID})
		e.requestReconcile()
	case *slidetypes.Disconnected:
		err = e.DB.SetState(ctx, e.Account, "realtime", "connection", map[string]any{"connected": false, "observed_at_ms": time.Now().UnixMilli(), "failures": v.FailureCount, "error": "Instagram connection interrupted; reconnect pending"})
		e.requestReconcile()
	case *slidetypes.AuthError:
		err = e.DB.SetState(ctx, e.Account, "account", "auth", store.State{Error: "Instagram authentication requires attention; run login"})
		e.fail(errors.New("Instagram authentication requires attention; run login"))
	case *slidetypes.ResnapshotRequired:
		var prior store.State
		err = e.DB.State(ctx, e.Account, "realtime", "checkpoint", &prior)
		if err == nil {
			err = e.DB.Transaction(ctx, func(tx *sql.Tx) error {
				gap := store.State{SeqID: prior.SeqID, Timestamp: time.Now().UnixMilli(), Gaps: []string{"source requested resnapshot; intervening realtime deltas may be unavailable; snapshots are reconciled separately"}}
				if err := store.SaveState(ctx, tx, e.Account, "reconciliation", fmt.Sprintf("resnapshot:%d", gap.Timestamp), gap); err != nil {
					return err
				}
				return store.SaveState(ctx, tx, e.Account, "realtime", "resnapshot", gap)
			})
		}
	}
	if err != nil {
		e.fail(err)
	}
	return err
}
func (e *Engine) PrepareRealtime(ctx context.Context) error {
	var state store.State
	if err := e.DB.State(ctx, e.Account, "realtime", "checkpoint", &state); err != nil {
		return err
	}
	if state.SeqID != 0 {
		e.Client.SetSeqID(state.SeqID, time.UnixMilli(state.Timestamp))
		return nil
	}
	// LoadIndex established the mailbox subscription baseline. Do not call it
	// again after restoring a durable checkpoint, since it resets the client cursor.
	if !e.Client.HasSeqID() {
		return errors.New("mailbox did not establish a realtime sequence baseline")
	}
	// The initial mailbox is only a baseline; history coverage is tracked separately.
	return e.DB.SetState(ctx, e.Account, "realtime", "baseline", map[string]any{"established_at_ms": time.Now().UnixMilli(), "coverage": "initial snapshot; preexisting realtime deltas unavailable"})
}
func (e *Engine) Follow(ctx context.Context) error {
	for ctx.Err() == nil {
		historyCtx, cancelHistory := context.WithCancel(ctx)
		historyDone := make(chan struct{})
		go func() {
			defer close(historyDone)
			for {
				if err := e.History(historyCtx); err != nil && historyCtx.Err() == nil {
					fmt.Fprintln(os.Stderr, "historical synchronization incomplete; inspect status")
				}
				timer := time.NewTimer(15 * time.Minute)
				select {
				case <-historyCtx.Done():
					timer.Stop()
					return
				case <-e.Reconcile:
					timer.Stop()
				case <-timer.C:
				}
				if historyCtx.Err() != nil {
					return
				}
			}
		}()
		e.Client.Connect(ctx)
		cancelHistory()
		<-historyDone
		if ctx.Err() != nil {
			break
		}
		var resnapshot store.State
		if err := e.DB.State(ctx, e.Account, "realtime", "resnapshot", &resnapshot); err != nil {
			return err
		}
		if len(resnapshot.Gaps) == 0 {
			return errors.New("Instagram realtime connection stopped; inspect status")
		}
		if err := e.Initialize(ctx); err != nil {
			return err
		}
		if err := e.DB.SetState(ctx, e.Account, "realtime", "resnapshot", store.State{LastSuccess: time.Now().UnixMilli()}); err != nil {
			return err
		}
		e.requestReconcile()
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

func (e *Engine) unparsedBatch(ctx context.Context, b []byte) error {
	raw, _ := json.Marshal(map[string]string{"unparseable_delta_batch": string(b)})
	err := e.archiveTransaction(ctx, func(tx *sql.Tx) error {
		if err := store.Put(ctx, tx, store.Record{Table: "deltas", Account: e.Account, Identity: "unparsed:" + store.Hash(b), Origin: "dgw-unparsed", Raw: raw}); err != nil {
			return err
		}
		return store.SaveState(ctx, tx, e.Account, "realtime", "gap", store.State{Error: "unparseable realtime batch; original bytes retained, checkpoint unchanged"})
	})
	if err == nil {
		err = errors.New("unparseable realtime batch; synchronization stopped with a recoverable gap")
	}
	e.fail(err)
	return err
}
