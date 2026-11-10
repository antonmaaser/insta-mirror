package projection

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.mau.fi/mautrix-meta/internal/store"
)

func WriteTranscript(ctx context.Context, db *store.Store, chat store.Record, w io.Writer) error {
	title := ""
	if chat.Thread.Title != nil {
		title = *chat.Thread.Title
	}
	var out strings.Builder
	fmt.Fprintf(&out, "Instagram mirror transcript\nAccount: %q\nChat: %d\nTitle: %q\nReference: %s\n", chat.Account, chat.ID, title, chat.Reference)
	if chat.Thread.Deleted {
		fmt.Fprintln(&out, "Thread deleted according to Instagram.")
	}
	for _, scope := range []string{"head", "thread"} {
		var state store.State
		if err := db.State(ctx, chat.Account, scope, strconv.FormatInt(chat.ID, 10), &state); err != nil {
			return err
		}
		if scope == "thread" {
			fmt.Fprintf(&out, "History pagination complete: %t\n", state.Complete)
		}
		if state.LastSuccess != 0 {
			fmt.Fprintf(&out, "Last %s page (UTC): %s\n", scope, time.UnixMilli(state.LastSuccess).UTC().Format(time.RFC3339))
		}
		if state.Error != "" {
			fmt.Fprintf(&out, "Acquisition error: %q\n", state.Error)
		}
		for _, gap := range state.Gaps {
			fmt.Fprintf(&out, "Coverage gap: %q\n", gap)
		}
	}
	fmt.Fprintln(&out)
	if _, err := io.WriteString(w, out.String()); err != nil {
		return err
	}
	return db.TranscriptMessages(ctx, chat.Account, chat.ID, func(r store.Record) error {
		m := r.Message
		var text strings.Builder
		stamp := "timestamp unavailable"
		if m.SentAt != 0 {
			stamp = time.UnixMilli(m.SentAt).UTC().Format(time.RFC3339Nano)
		}
		sender := m.Sender
		if sender == "" {
			sender = "unknown sender"
		}
		fmt.Fprintf(&text, "[%s] %q", stamp, sender)
		if m.SenderID != nil {
			fmt.Fprintf(&text, " (user %d)", *m.SenderID)
		}
		fmt.Fprintln(&text)
		body := "[content unavailable]"
		switch {
		case m.Deleted:
			body = "[deleted]"
		case m.Body != nil:
			body = *m.Body
		case m.Snippet != nil:
			body = *m.Snippet
		case m.Kind != nil:
			body = "[" + *m.Kind + "]"
		}
		fmt.Fprintln(&text, transcriptText(body))
		if m.Reply != nil {
			fmt.Fprintf(&text, "Reply to message %d", m.Reply.ID)
			if !m.Reply.Available {
				fmt.Fprint(&text, " (content unavailable)")
			}
			fmt.Fprintln(&text)
		}
		for _, reaction := range m.Reactions {
			fmt.Fprintf(&text, "Reaction: %q by user %d\n", reaction.Value, reaction.UserID)
		}
		for _, a := range m.Attachments {
			status := "not cached"
			if a.Restricted {
				status = "restricted"
			}
			fmt.Fprintf(&text, "Attachment: %q (%s)\n", a.Kind, status)
		}
		items, err := db.Media(ctx, r.Account, r.ID)
		if err != nil {
			return err
		}
		for _, item := range items {
			fmt.Fprintf(&text, "Media: %q status=%s mime=%q bytes=%d resource=%s\n", item.Attachment, item.Status, item.MIME, item.Size, item.Resource)
		}
		fmt.Fprintf(&text, "Reference: %s\n\n", r.Reference)
		_, err = io.WriteString(w, text.String())
		return err
	})
}

func transcriptText(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, text)
}
