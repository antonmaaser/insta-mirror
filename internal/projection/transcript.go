package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"

	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

// WriteTranscript projects locally acquired observations without persisting a
// canonical conversation. The caller holds one read snapshot for the export.
func WriteTranscript(ctx context.Context, db *store.Store, chat store.Record, w io.Writer) error {
	var thread slidetypes.ThreadInfo
	decodeErr := json.Unmarshal(chat.Raw, &thread)
	names := map[string]string{}
	users := append([]*slidetypes.User{}, thread.Users...)
	users = append(users, thread.Viewer)
	for _, user := range users {
		if user == nil || user.InteropMessagingUserFBID == 0 {
			continue
		}
		name := user.FullName
		if name == "" {
			name = user.Username
		}
		if name != "" {
			names[strconv.FormatInt(user.InteropMessagingUserFBID, 10)] = name
		}
	}
	var coverage store.State
	if thread.ThreadFBID != "" {
		if err := db.State(ctx, chat.Account, "thread", thread.ThreadFBID, &coverage); err != nil {
			return err
		}
	}
	var header strings.Builder
	fmt.Fprintf(&header, "Instagram mirror transcript\nAccount: %q\nChat: %q\nTitle: %q\nGenerated at (UTC): %s\nThread source: %s\n", chat.Account, chat.Identity, store.String(chat.Raw, "thread_title"), time.Now().UTC().Format(time.RFC3339Nano), store.SourceRef(chat))
	fmt.Fprintf(&header, "History exhausted at source: %t\n", coverage.Complete)
	if coverage.LastSuccess != 0 {
		fmt.Fprintf(&header, "Last acquired history page (UTC): %s\n", time.UnixMilli(coverage.LastSuccess).UTC().Format(time.RFC3339Nano))
	}
	if coverage.Error != "" {
		fmt.Fprintf(&header, "History error: %q\n", coverage.Error)
	}
	for _, gap := range coverage.Gaps {
		fmt.Fprintf(&header, "Coverage gap: %q\n", gap)
	}
	if decodeErr != nil {
		fmt.Fprintln(&header, "Thread interpretation is partial; the source record remains available.")
	}
	fmt.Fprint(&header, "\nThis is the local archive at one read snapshot, not a claim of complete Instagram history.\nMessages use source timestamps; ties use local record order. Missing timestamps are listed last.\nNames come from the retained thread observation. Updates are applied only where source ordering is established.\n")
	if _, err := io.WriteString(w, header.String()); err != nil {
		return err
	}
	count := 0
	err := db.TranscriptMessages(ctx, chat.Account, chat.Identity, func(record store.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		projected, err := Build(ctx, db, record, true)
		if err != nil {
			return err
		}
		_, err = io.WriteString(w, transcriptMessage(projected, names))
		count++
		return err
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "Messages exported: %d\n", count)
	return err
}
func transcriptMessage(result Result, names map[string]string) string {
	value := result.Snapshot
	if result.Reconstructed != nil {
		value = result.Reconstructed
	}
	raw, _ := json.Marshal(value)
	var message slidetypes.Message
	decodeErr := json.Unmarshal(raw, &message)
	senderID := strconv.FormatInt(message.SenderFBID, 10)
	name := names[senderID]
	if message.SenderFBID == 0 {
		senderID = "unavailable"
	}
	if message.Sender != nil && message.Sender.Name != "" {
		name = message.Sender.Name
	}
	if name == "" {
		name = "unknown sender"
	}
	timestamp := "source timestamp unavailable"
	if !message.TimestampMS.IsZero() {
		timestamp = message.TimestampMS.UTC().Format(time.RFC3339Nano)
	}
	body := message.TextBody
	switch content := message.Content.Content.(type) {
	case *slidetypes.MessageContentText:
		if body == "" {
			body = content.TextBody
		}
	case *slidetypes.MessageContentAdminText:
		if body == "" {
			for _, fragment := range content.TextFragments {
				body += fragment.Plaintext
				if fragment.LinkFragment != nil {
					body += " " + fragment.LinkFragment.URI
				}
			}
		}
	case *slidetypes.MessageContentXMA:
		if body == "" {
			body = content.XMATextBody
			if content.XMA != nil {
				if content.XMA.TitleText != "" {
					body += "\n" + content.XMA.TitleText
				}
				if content.XMA.CaptionBodyText != "" {
					body += "\n" + content.XMA.CaptionBodyText
				}
			}
		}
	}
	if strings.TrimSpace(body) == "" {
		kind := message.Content.TypeName
		if kind == "" {
			kind = "content unavailable"
		}
		body = "[" + kind + "]"
	}
	var out strings.Builder
	fmt.Fprintf(&out, "[%s] %q (sender_fbid:%s)\n", timestamp, name, senderID)
	if result.Deleted {
		fmt.Fprintln(&out, "[Deleted or unsent according to a subsequent source update; retained text follows.]")
	}
	fmt.Fprintln(&out, transcriptText(body))
	if message.RepliedToMessageID != "" {
		fmt.Fprintf(&out, "Reply to source message: %q\n", message.RepliedToMessageID)
	}
	for _, reaction := range message.Reactions {
		if reaction != nil {
			fmt.Fprintf(&out, "Reaction: %q by sender_fbid:%d\n", reaction.Reaction, reaction.SenderFBID)
		}
	}
	for _, media := range result.Media {
		fmt.Fprintf(&out, "Media: %q status=%s mime=%q bytes=%d resource=%s\n", media.Attachment, media.Status, media.MIME, media.Size, media.Resource)
	}
	fmt.Fprintf(&out, "Source: %s (observed %s)\n", result.Reference, time.UnixMilli(result.Source.Observed).UTC().Format(time.RFC3339Nano))
	if decodeErr != nil {
		fmt.Fprintln(&out, "Uncertainty: upstream message decoding failed; consult the source record.")
	}
	for _, uncertainty := range result.Uncertainty {
		fmt.Fprintf(&out, "Uncertainty: %s\n", uncertainty)
	}
	for _, record := range result.Updates {
		var delta slidetypes.Delta
		if err := json.Unmarshal(record.Raw, &delta); err != nil {
			fmt.Fprintf(&out, "Unparsed update: %s\n", store.SourceRef(record))
			continue
		}
		fmt.Fprintf(&out, "Observed update: %q sequence=%d source=%s", delta.TypeName, delta.UQSeqID, store.SourceRef(record))
		switch event := delta.Data.(type) {
		case *slidetypes.EditMessageEvent:
			fmt.Fprintf(&out, " text=%q", event.TextBody)
		case *slidetypes.CreateReactionEvent:
			fmt.Fprintf(&out, " reaction=%q sender_fbid=%d", event.Reaction.Reaction, event.Reaction.SenderFBID)
		case *slidetypes.DeleteReactionEvent:
			fmt.Fprintf(&out, " reaction=%q sender_fbid=%d", event.Reaction.Reaction, event.Reaction.SenderFBID)
		}
		fmt.Fprintln(&out)
	}
	fmt.Fprintln(&out)
	return out.String()
}
func transcriptText(text string) string {
	var out strings.Builder
	for _, r := range text {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			fmt.Fprintf(&out, "\\u%04x", r)
		} else {
			out.WriteRune(r)
		}
	}
	return out.String()
}
