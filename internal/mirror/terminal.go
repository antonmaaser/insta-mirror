package mirror

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"go.mau.fi/mautrix-meta/internal/projection"
	"go.mau.fi/mautrix-meta/internal/store"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
	"golang.org/x/text/width"
)

const (
	keyUp byte = 128 + iota
	keyDown
	keyLeft
	keyRight
	keyEscape
)

type terminalKeys struct {
	pending []byte
	escape  time.Time
}

func (k *terminalKeys) read(ctx context.Context, fd int) (byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(k.pending) > 0 {
		if k.pending[0] != 27 {
			k.escape = time.Time{}
			key := k.pending[0]
			k.pending = k.pending[1:]
			return key, nil
		}
		if k.escape.IsZero() {
			k.escape = time.Now()
		}
		if len(k.pending) < 3 && time.Since(k.escape) >= 100*time.Millisecond {
			k.pending = nil
			return keyEscape, nil
		}
		if len(k.pending) > 1 && k.pending[1] != '[' && k.pending[1] != 'O' {
			k.pending = nil
			return keyEscape, nil
		}
		for i := 2; i < len(k.pending); i++ {
			b := k.pending[i]
			if b >= 0x40 && b <= 0x7e {
				k.pending = k.pending[i+1:]
				k.escape = time.Time{}
				switch b {
				case 'A':
					return keyUp, nil
				case 'B':
					return keyDown, nil
				case 'C':
					return keyRight, nil
				case 'D':
					return keyLeft, nil
				}
				return 0, nil
			}
		}
	}
	// select supports macOS /dev/tty, which reports POLLNVAL through poll.
	var fds unix.FdSet
	if fd < 0 || fd >= len(fds.Bits)*unix.NFDBITS {
		return 0, errors.New("terminal descriptor exceeds select capacity")
	}
	fds.Set(fd)
	timeout := unix.NsecToTimeval(int64(50 * time.Millisecond))
	ready, err := unix.Select(fd+1, &fds, nil, nil, &timeout)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		return 0, err
	}
	if ready > 0 && fds.IsSet(fd) {
		var b [64]byte
		n, err := unix.Read(fd, b[:])
		if err != nil {
			if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
				return 0, nil
			}
			return 0, err
		}
		if n == 0 {
			return 0, io.EOF
		}
		if len(k.pending) == 0 && b[0] == 27 {
			k.escape = time.Now()
		}
		k.pending = append(k.pending, b[:n]...)
	}
	return 0, nil
}

func runeCells(r rune, column int) int {
	if r == '\t' {
		return 4 - column%4
	}
	if unicode.IsControl(r) {
		return 1
	}
	if unicode.Is(unicode.Mn, r) || unicode.Is(unicode.Me, r) || r == '\u200d' {
		return 0
	}
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return 2
	}
	return 1
}
func terminalLine(text string, columns int) string {
	if columns <= 0 {
		return ""
	}
	var out strings.Builder
	cells := 0
	for _, r := range text {
		if unicode.IsControl(r) && r != '\t' {
			r = ' '
		}
		n := runeCells(r, cells)
		if cells+n > columns {
			break
		}
		if r == '\t' {
			out.WriteString(strings.Repeat(" ", n))
		} else {
			out.WriteRune(r)
		}
		cells += n
	}
	return out.String()
}
func chatName(row store.ChatRow) string {
	if strings.TrimSpace(row.Title) != "" {
		return row.Title
	}
	var names []string
	for _, member := range row.Members {
		name := member.Name
		if name != "" {
			names = append(names, name)
		}
		if len(names) == 4 {
			break
		}
	}
	if len(names) > 0 {
		return strings.Join(names, ", ")
	}
	return "Chat " + row.Identity
}

type transcriptView struct {
	file    *os.File
	offsets []int64
	columns int
}

func (v *transcriptView) close() { v.file.Close(); os.Remove(v.file.Name()) }
func (v *transcriptView) reflow(ctx context.Context, columns int) error {
	if _, err := v.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReader(v.file)
	v.offsets = []int64{0}
	v.columns = columns
	var pos int64
	cells := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, n, err := reader.ReadRune()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if r == '\n' {
			pos += int64(n)
			v.offsets = append(v.offsets, pos)
			cells = 0
			continue
		}
		size := runeCells(r, cells)
		if cells+size > columns && cells > 0 {
			v.offsets = append(v.offsets, pos)
			cells = 0
			size = runeCells(r, 0)
		}
		pos += int64(n)
		cells += size
	}
	if v.offsets[len(v.offsets)-1] != pos {
		v.offsets = append(v.offsets, pos)
	}
	return nil
}
func (v *transcriptView) line(index int) (string, error) {
	if index < 0 || index+1 >= len(v.offsets) {
		return "", nil
	}
	length := v.offsets[index+1] - v.offsets[index]
	// Combining-only source lines need not allocate unbounded display buffers.
	if length > 64<<10 {
		length = 64 << 10
	}
	b := make([]byte, int(length))
	n, err := v.file.ReadAt(b, v.offsets[index])
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return strings.TrimSuffix(string(b[:n]), "\n"), nil
}
func loadTranscriptView(ctx context.Context, db *store.Store, account, identity string, columns int) (*transcriptView, error) {
	f, err := os.CreateTemp("", "igdm-transcript-")
	if err != nil {
		return nil, err
	}
	v := &transcriptView{file: f}
	ok := false
	defer func() {
		if !ok {
			v.close()
		}
	}()
	err = db.ReadSnapshot(ctx, func(snapshot *store.Store) error {
		chat, err := selectedChat(ctx, snapshot, account, identity)
		if err != nil {
			return err
		}
		w := bufio.NewWriter(f)
		if err = projection.WriteTranscript(ctx, snapshot, chat, w); err != nil {
			return err
		}
		return w.Flush()
	})
	if err != nil {
		return nil, err
	}
	if err = v.reflow(ctx, columns); err != nil {
		return nil, err
	}
	ok = true
	return v, nil
}

type terminalResult struct {
	kind string
	rows []store.ChatRow
	view *transcriptView
	path string
	err  error
}

func runTerminal(ctx context.Context, db *store.Store, dbPath, account, exportDir string) (runErr error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("cmd requires an interactive terminal; use cmd chats or cmd export for direct commands: %w", err)
	}
	defer tty.Close()
	fd := int(tty.Fd())
	state, err := term.MakeRaw(fd)
	if err != nil {
		return err
	}
	defer func() {
		if err := term.Restore(fd, state); runErr == nil && err != nil {
			runErr = err
		}
	}()
	fmt.Fprint(tty, "\x1b[?1049h\x1b[?25l\x1b[?7l")
	defer fmt.Fprint(tty, "\x1b[?7h\x1b[?25h\x1b[?1049l")
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan terminalResult, 1)
	busy := false
	var rows []store.ChatRow
	var view *transcriptView
	focus, top, scroll := 0, 0, 0
	identity := ""
	status := ""
	columns, height := 0, 0
	dirty := true
	defer func() {
		cancel()
		if busy {
			result := <-results
			if result.view != nil {
				result.view.close()
			}
		}
		if view != nil {
			view.close()
		}
	}()
	start := func(kind string, operation func() terminalResult) {
		busy = true
		status = kind + "…"
		dirty = true
		go func() { result := operation(); result.kind = kind; results <- result }()
	}
	overview := func() terminalResult {
		result := terminalResult{}
		result.err = db.ReadSnapshot(workCtx, func(snapshot *store.Store) error {
			var err error
			result.rows, err = snapshot.Chats(workCtx, account)
			return err
		})
		for i := range result.rows {
			result.rows[i].Title = chatName(result.rows[i])
			result.rows[i].Members = nil
		}
		return result
	}
	start("Loading chats", overview)
	keys := terminalKeys{}
	for {
		if err = workCtx.Err(); err != nil {
			return err
		}
		select {
		case result := <-results:
			busy = false
			dirty = true
			if result.err != nil {
				status = "Error: " + result.err.Error()
				break
			}
			switch result.kind {
			case "Loading chats":
				rows = result.rows
				focus = 0
				for i, row := range rows {
					if row.Identity == identity {
						focus = i
						break
					}
				}
				top = 0
				status = ""
			case "Loading transcript":
				if view != nil {
					view.close()
				}
				view = result.view
				scroll = 0
				status = ""
			case "Exporting":
				status = "Exported: " + filepath.Base(result.path)
			}
		default:
		}
		w, h, sizeErr := term.GetSize(fd)
		if sizeErr != nil {
			return sizeErr
		}
		w = max(w, 4)
		h = max(h, 4)
		if w != columns || h != height {
			columns, height = w, h
			dirty = true
			if view != nil && view.columns != columns-1 {
				anchor := view.offsets[min(scroll, len(view.offsets)-1)]
				if err = view.reflow(workCtx, columns-1); err != nil {
					return err
				}
				scroll = sort.Search(len(view.offsets), func(i int) bool { return view.offsets[i] >= anchor })
			}
		}
		visible := max(1, height-3)
		if dirty {
			var screen strings.Builder
			screen.WriteString("\x1b[H\x1b[2J")
			title := "Chats"
			if view != nil && len(rows) > 0 {
				title = chatName(rows[focus])
			}
			fmt.Fprintf(&screen, "%s\r\n", terminalLine(title+"  | account "+account, columns-1))
			if view == nil {
				if focus < top {
					top = focus
				}
				if focus >= top+visible {
					top = focus - visible + 1
				}
				for i := 0; i < visible; i++ {
					index := top + i
					text := ""
					if index < len(rows) {
						prefix := "  "
						if index == focus {
							prefix = "> "
						}
						text = terminalLine(prefix+chatName(rows[index]), max(0, columns-1))
					} else if len(rows) == 0 && i == 0 && !busy {
						text = "No mirrored chats"
					}
					fmt.Fprintf(&screen, "%s\r\n", terminalLine(text, columns-1))
				}
			} else {
				scroll = max(0, min(scroll, max(0, len(view.offsets)-1-visible)))
				for i := 0; i < visible; i++ {
					line, readErr := view.line(scroll + i)
					if readErr != nil {
						return readErr
					}
					fmt.Fprintf(&screen, "%s\r\n", terminalLine(line, columns-1))
				}
			}
			hint := "↑/↓ select  → inspect  e export  q/Esc quit"
			if view != nil {
				hint = "↑/↓ scroll  ← chats  e export  q/Esc quit"
			}
			fmt.Fprintf(&screen, "%s\r\n%s", terminalLine(hint, columns-1), terminalLine(status, columns-1))
			if _, err = tty.WriteString(screen.String()); err != nil {
				return err
			}
			dirty = false
		}
		key, keyErr := keys.read(workCtx, fd)
		if keyErr != nil {
			if errors.Is(keyErr, io.EOF) {
				return nil
			}
			return keyErr
		}
		if key == 'q' || key == 'Q' || key == keyEscape || key == 3 || key == 4 {
			return nil
		}
		if busy {
			continue
		}
		switch key {
		case keyUp:
			if view == nil {
				focus = max(0, focus-1)
			} else {
				scroll = max(0, scroll-1)
			}
			dirty = true
		case keyDown:
			if view == nil {
				focus = min(max(0, len(rows)-1), focus+1)
			} else {
				scroll++
			}
			dirty = true
		case keyLeft:
			if view != nil {
				view.close()
				view = nil
				identity = rows[focus].Identity
				start("Loading chats", overview)
			}
		case keyRight:
			if view == nil && len(rows) > 0 {
				identity = rows[focus].Identity
				cols := columns - 1
				start("Loading transcript", func() terminalResult {
					v, err := loadTranscriptView(workCtx, db, account, identity, cols)
					return terminalResult{view: v, err: err}
				})
			}
		case 'e', 'E':
			if len(rows) > 0 {
				selected := rows[focus].Identity
				start("Exporting", func() terminalResult {
					path, err := uniqueTranscriptPath(exportDir)
					if err == nil {
						err = db.ReadSnapshot(workCtx, func(snapshot *store.Store) error {
							chat, err := selectedChat(workCtx, snapshot, account, selected)
							if err != nil {
								return err
							}
							return exportTranscript(workCtx, snapshot, dbPath, chat, path)
						})
					}
					return terminalResult{path: path, err: err}
				})
			}
		}
	}
}
func uniqueTranscriptPath(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return "", err
	}
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return filepath.Join(dir, "transcript-"+time.Now().UTC().Format("20060102T150405.000000000Z")+"-"+hex.EncodeToString(suffix[:])+".txt"), nil
}
