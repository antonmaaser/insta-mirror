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

func (e *Engine) captureDeltas(ctx context.Context, raw []byte) error {
	var envelopes []json.RawMessage
	if json.Unmarshal(raw, &envelopes) != nil || envelopes == nil {
		return e.blockRealtime(ctx, errors.New("malformed realtime batch"))
	}
	err := e.ingestTransaction(ctx, "realtime", func(b *ingestion) error {
		var state store.State
		if err := store.ReadState(ctx, b.tx, e.Account, "realtime", "checkpoint", &state); err != nil {
			return err
		}
		start := state.SeqID
		for _, envelope := range envelopes {
			var items []json.RawMessage
			if json.Unmarshal(field(envelope, "data", "slide_delta_processor"), &items) != nil || items == nil {
				return errors.New("malformed realtime batch")
			}
			for _, item := range items {
				var delta slidetypes.Delta
				if json.Unmarshal(item, &delta) != nil || delta.UQSeqID <= 0 {
					return errors.New("invalid or unsequenced realtime event")
				}
				var obj sourceObject
				if json.Unmarshal(item, &obj) != nil {
					return errors.New("invalid realtime event")
				}
				digest := store.Hash([]byte(store.JSONValue(obj)))
				switch {
				case delta.UQSeqID < state.SeqID:
					if delta.UQSeqID > start {
						return errors.New("realtime batch is out of order")
					}
					b.decisions = append(b.decisions, store.Decision{Kind: "event", Action: "ignored_stale", Reason: "sequence already passed"})
				case delta.UQSeqID == state.SeqID:
					if state.PageHash != "" && state.PageHash != digest {
						return store.ErrConflict
					}
					b.decisions = append(b.decisions, store.Decision{Kind: "event", Action: "unchanged", Reason: "replayed sequence"})
				default:
					b.event = state.Revision + 1
					if err := b.delta(item, delta); err != nil {
						return err
					}
					state.SeqID, state.PageHash, state.Revision = delta.UQSeqID, digest, b.event
				}
			}
		}
		if state.SeqID == start {
			return nil
		}
		state.Timestamp = time.Now().UnixMilli()
		state.LastSuccess = state.Timestamp
		state.Error = ""
		return store.SaveState(ctx, b.tx, e.Account, "realtime", "checkpoint", state)
	})
	if err != nil {
		return e.blockRealtime(ctx, err)
	}
	return nil
}

func (e *Engine) blockRealtime(ctx context.Context, cause error) error {
	var state store.State
	if err := e.DB.State(ctx, e.Account, "realtime", "checkpoint", &state); err != nil {
		e.fail(err)
		return err
	}
	state.Error = cause.Error()
	if err := e.DB.SetState(ctx, e.Account, "realtime", "checkpoint", state); err != nil {
		e.fail(err)
		return err
	}
	e.fail(cause)
	return cause
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
		// The raw batch callback applies deltas and progress together.
		return nil
	case *slidetypes.SeqIDUpdate:
		// captureDeltas committed the checkpoint with its changes. The
		// transport notification must not overwrite it or advance it separately.
		return nil
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
				if err := store.SaveState(ctx, tx, e.Account, "reconciliation", "resnapshot", gap); err != nil {
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
		if err := e.PrepareRealtime(ctx); err != nil {
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
