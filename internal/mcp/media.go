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
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/internal/store"
)

const mediaPageSize int64 = 50

//go:embed media.html
var mediaHTML string

var mediaTemplate = template.Must(template.New("media").Funcs(template.FuncMap{
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
	page := int64(1)
	if raw := r.URL.Query().Get("page"); raw != "" {
		var err error
		page, err = strconv.ParseInt(raw, 10, 32)
		if err != nil || page < 1 {
			http.Error(w, "invalid page", http.StatusBadRequest)
			return
		}
	}
	items, err := s.DB.ListMedia(r.Context(), s.Account, mediaPageSize+1, (page-1)*mediaPageSize)
	if err != nil {
		http.Error(w, "could not list cached media", http.StatusInternalServerError)
		return
	}
	view := struct {
		Items    []store.MediaListing
		Previous int64
		Next     int64
	}{Previous: page - 1}
	if int64(len(items)) > mediaPageSize {
		view.Next = page + 1
		items = items[:mediaPageSize]
	}
	for _, item := range items {
		if s.Cache.HasFile(item.Hash) {
			view.Items = append(view.Items, item)
		}
	}

	var out bytes.Buffer
	if err := mediaTemplate.Execute(&out, view); err != nil {
		http.Error(w, "could not display cached media", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'")
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

	name := "instagram-" + item.Hash[:12]
	contentType, _, err := mime.ParseMediaType(item.MIME)
	if err != nil {
		contentType = "application/octet-stream"
	}
	if extensions, _ := mime.ExtensionsByType(contentType); len(extensions) > 0 {
		name += extensions[0]
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	http.ServeContent(w, r, name, time.Time{}, file)
}
