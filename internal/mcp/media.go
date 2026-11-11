package mcp

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"html/template"
	"mime"
	"net/http"
	"os"
	"strings"
	"time"

	"go.mau.fi/mautrix-meta/internal/store"
)

//go:embed media.html
var mediaHTML string

//go:embed media-sort.js
var mediaSortJS []byte

var mediaTemplate = template.Must(template.New("media").Funcs(template.FuncMap{
	"filename": mediaFilename,
	"size": func(size int64) string {
		value := float64(size)
		for _, unit := range []string{"KiB", "MiB", "GiB"} {
			if value < 1024 {
				break
			}
			value /= 1024
			if value < 1024 || unit == "GiB" {
				return fmt.Sprintf("%.1f %s", value, unit)
			}
		}
		return fmt.Sprintf("%d B", size)
	},
}).Parse(mediaHTML))

func (s *Server) showMedia(w http.ResponseWriter, r *http.Request) {
	if s.Cache == nil {
		http.Error(w, "media cache unavailable", http.StatusServiceUnavailable)
		return
	}
	items, err := s.DB.ListMedia(r.Context(), s.Account)
	if err != nil {
		http.Error(w, "could not list cached media", http.StatusInternalServerError)
		return
	}
	available := items[:0]
	for _, item := range items {
		if s.Cache.HasFile(item.Hash) {
			available = append(available, item)
		}
	}

	var out bytes.Buffer
	if err := mediaTemplate.Execute(&out, available); err != nil {
		http.Error(w, "could not display cached media", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'")
	_, _ = w.Write(out.Bytes())
}

func (s *Server) downloadMedia(w http.ResponseWriter, r *http.Request) {
	if s.Cache == nil {
		http.Error(w, "media cache unavailable", http.StatusServiceUnavailable)
		return
	}
	file, item, err := s.Cache.OpenAvailable(r.Context(), s.Account, r.PathValue("hash"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || store.NoMedia(err) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "could not open cached media", http.StatusInternalServerError)
		}
		return
	}
	defer file.Close()

	name := mediaFilename(item.MIME, item.Hash)
	contentType := mediaContentType(item.MIME)
	disposition := "attachment"
	if r.URL.Query().Get("view") == "1" && inlineMediaType(contentType) {
		disposition = "inline"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType(disposition, map[string]string{"filename": name}))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, name, time.Time{}, file)
}

func mediaContentType(value string) string {
	contentType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return "application/octet-stream"
	}
	return contentType
}

func mediaFilename(contentType, hash string) string {
	name := "instagram-" + hash[:12]
	if extensions, _ := mime.ExtensionsByType(mediaContentType(contentType)); len(extensions) > 0 {
		name += extensions[0]
	}
	return name
}

func inlineMediaType(contentType string) bool {
	switch contentType {
	case "image/apng", "image/avif", "image/bmp", "image/gif", "image/jpeg", "image/png", "image/webp":
		return true
	default:
		return strings.HasPrefix(contentType, "audio/") || strings.HasPrefix(contentType, "video/")
	}
}
