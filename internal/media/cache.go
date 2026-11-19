package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"go.mau.fi/mautrix-meta/internal/store"
	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/messagix/useragent"
)

const MaxSize int64 = 256 << 20
const ChunkSize int64 = 4 << 20

type Cache struct {
	DB   *store.Store
	Dir  string
	HTTP *http.Client
}

func New(db *store.Store, dir string) (*Cache, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if !ip.IP.IsGlobalUnicast() || ip.IP.IsPrivate() || ip.IP.IsLoopback() || ip.IP.IsLinkLocalUnicast() {
				return nil, errors.New("nonpublic media endpoint")
			}
		}
		var last error
		for _, ip := range ips {
			conn, err := (&net.Dialer{Timeout: 20 * time.Second}).DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		if last == nil {
			last = errors.New("media host has no address")
		}
		return nil, last
	}, TLSHandshakeTimeout: 20 * time.Second, ResponseHeaderTimeout: 30 * time.Second}
	c := &Cache{DB: db, Dir: dir, HTTP: &http.Client{Transport: transport, Timeout: 5 * time.Minute}}
	c.HTTP.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 || !allowed(req.URL) {
			return errors.New("media redirect denied")
		}
		return nil
	}
	return c, nil
}
func (c *Cache) Close() { c.HTTP.CloseIdleConnections() }
func allowed(u *url.URL) bool {
	if u.Scheme != "https" || u.User != nil || u.Port() != "" && u.Port() != "443" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, domain := range []string{"fbcdn.net", "cdninstagram.com", "giphy.com"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}
func DescribeAttachments(msg slidetypes.Message, expiring bool) ([]store.Attachment, error) {
	var describeErr error
	out := []store.Attachment{}
	add := func(ref string, urls ...string) {
		nonempty := []string{}
		for _, u := range urls {
			if u != "" {
				nonempty = append(nonempty, u)
			}
		}
		if len(nonempty) > 0 || ref != "" && ref != "fbid:" {
			if ref == "" || ref == "fbid:" {
				var err error
				ref, err = store.MediaKey("", nonempty)
				if err != nil {
					describeErr = err
					return
				}
			}
			out = append(out, store.Attachment{Key: ref, URLs: nonempty, Kind: msg.ContentType})
		}
	}
	ordinary := func(items []*slidetypes.Attachment) {
		for _, a := range items {
			if a != nil {
				before := len(out)
				add("fbid:"+a.AttachmentFBID, a.AttachmentCDNURL, a.AttachmentCDNFallbackURL)
				if len(out) > before {
					if a.AttachmentFBID != "" {
						remote := a.AttachmentFBID
						out[len(out)-1].RemoteEntityID = &remote
					}
					typ := int64(a.AttachmentType)
					out[len(out)-1].RemoteEntityType = &typ
				}
			}
		}
	}
	switch content := msg.Content.Content.(type) {
	case *slidetypes.MessageContentImage:
		ordinary(content.Attachments)
	case *slidetypes.MessageContentVideo:
		ordinary(content.Videos)
	case *slidetypes.MessageContentMultiMedia:
		ordinary(content.Attachments)
	case *slidetypes.MessageContentAudio:
		for _, a := range content.AudioAttachments {
			if a != nil {
				add("fbid:"+a.AttachmentFBID, a.AttachmentCDNURL)
			}
		}
	case *slidetypes.MessageContentAnimatedMedia:
		for _, a := range content.AnimatedMedia {
			if a != nil {
				add("", a.AttachmentMP4URL, a.AttachmentWebpURL)
			}
		}
	case *slidetypes.MessageContentSticker:
		add("", content.PreviewURL)
	case *slidetypes.MessageContentXMA:
		if content.XMA != nil {
			image := content.XMA.PreviewImage
			if image == nil {
				image = content.XMA.XMAPreviewImage
			}
			if image != nil {
				before := len(out)
				add("", image.URL, image.FallbackURL)
				if len(out) > before {
					a := &out[len(out)-1]
					x := content.XMA
					if x.TargetID != "" {
						a.RemoteEntityID = &x.TargetID
					}
					if x.TitleText != "" {
						a.Title = &x.TitleText
					} else if x.XMATitle != "" {
						a.Title = &x.XMATitle
					}
					if x.SubtitleText != "" {
						a.Subtitle = &x.SubtitleText
					}
					if x.TargetURL != "" {
						a.TargetURL = &x.TargetURL
					}
				}
			}
		}
	case *slidetypes.MessageContentRavenImage:
		before := len(out)
		ordinary([]*slidetypes.Attachment{content.Attachment})
		if len(out) == before {
			out = append(out, store.Attachment{Kind: msg.ContentType})
		}
		for i := before; i < len(out); i++ {
			out[i].Restricted = true
			out[i].URLs = nil
		}
	case *slidetypes.MessageContentRavenVideo:
		before := len(out)
		ordinary([]*slidetypes.Attachment{content.Attachment})
		if len(out) == before {
			out = append(out, store.Attachment{Kind: msg.ContentType})
		}
		for i := before; i < len(out); i++ {
			out[i].Restricted = true
			out[i].URLs = nil
		}
	}
	if expiring {
		for i := range out {
			out[i].Restricted = true
			out[i].URLs = nil
		}
	}
	return out, describeErr
}
func (c *Cache) Sync(ctx context.Context, account string) error {
	var after int64
	for ctx.Err() == nil {
		assets, err := c.DB.PendingMedia(ctx, account, after)
		if err != nil {
			return err
		}
		if len(assets) == 0 {
			return nil
		}
		for _, m := range assets {
			m.Attempts++
			m.Attempted = time.Now().UnixMilli()
			m.Status = "pending"
			m.Error = "media retrieval failed; retry pending"
			for attempt := 0; attempt < 3; attempt++ {
				permanent := true
				for _, raw := range m.URLs {
					hash, mime, size, terminal, err := c.download(ctx, raw)
					if err == nil {
						m.Status = "available"
						m.Hash = hash
						m.MIME = mime
						m.Size = size
						m.Error = ""
						break
					}
					permanent = permanent && terminal
				}
				if m.Status == "available" {
					break
				}
				if permanent {
					m.Status = "unavailable"
					m.Error = "source media is inaccessible, unsupported, or exceeds the cache limit"
					break
				}
				timer := time.NewTimer(time.Duration(1<<attempt) * time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
			if err = c.DB.SaveMedia(ctx, m); err != nil {
				return err
			}
			after = m.AssetID
		}
	}
	return ctx.Err()
}
func (c *Cache) HasFile(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	if _, err := hex.DecodeString(hash); err != nil {
		return false
	}
	info, err := os.Lstat(filepath.Join(c.Dir, hash))
	return err == nil && info.Mode().IsRegular()
}
func (c *Cache) download(ctx context.Context, source string) (string, string, int64, bool, error) {
	u, err := url.Parse(source)
	if err != nil || !allowed(u) {
		return "", "", 0, true, errors.New("unsupported media host")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", "", 0, true, err
	}
	req.Header.Set("User-Agent", useragent.UserAgent)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Sec-Fetch-Mode", "no-cors")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", "", 0, false, errors.New("media transport failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", 0, resp.StatusCode == 401 || resp.StatusCode == 403 || resp.StatusCode == 404 || resp.StatusCode == 410, errors.New("media source rejected retrieval")
	}
	if resp.ContentLength > MaxSize {
		return "", "", 0, true, errors.New("media exceeds cache limit")
	}
	f, err := os.CreateTemp(c.Dir, ".download-")
	if err != nil {
		return "", "", 0, false, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, hash), io.LimitReader(resp.Body, MaxSize+1))
	if err != nil {
		return "", "", 0, false, err
	}
	if size == 0 || size > MaxSize {
		return "", "", 0, true, errors.New("empty or oversized media")
	}
	if resp.ContentLength >= 0 && size != resp.ContentLength {
		return "", "", 0, false, errors.New("incomplete media")
	}
	if err = f.Sync(); err != nil {
		return "", "", 0, false, err
	}
	if err = f.Close(); err != nil {
		return "", "", 0, false, err
	}
	mime, err := mimetype.DetectFile(f.Name())
	if err != nil {
		return "", "", 0, false, err
	}
	if mime.Is("text/html") || mime.Is("application/json") {
		return "", "", 0, true, errors.New("source returned a document instead of media")
	}
	contentHash := hex.EncodeToString(hash.Sum(nil))
	if err = os.Rename(f.Name(), filepath.Join(c.Dir, contentHash)); err != nil {
		return "", "", 0, false, err
	}
	d, err := os.Open(c.Dir)
	if err != nil {
		return "", "", 0, false, err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return "", "", 0, false, err
	}
	return contentHash, mime.String(), size, false, nil
}

// OpenAvailable opens a cached file only if it belongs to an available
// attachment for this account. The caller closes the file.
func (c *Cache) OpenAvailable(ctx context.Context, account, hash string) (*os.File, store.Media, error) {
	if !c.HasFile(hash) {
		return nil, store.Media{}, os.ErrNotExist
	}
	m, err := c.DB.MediaHash(ctx, account, hash)
	if err != nil {
		return nil, m, err
	}
	f, err := os.Open(filepath.Join(c.Dir, hash))
	return f, m, err
}

func (c *Cache) Read(ctx context.Context, account, hash string, offset, length int64) ([]byte, store.Media, error) {
	if offset < 0 || length < 1 || length > ChunkSize {
		return nil, store.Media{}, fmt.Errorf("media length must be 1 through %d bytes", ChunkSize)
	}
	f, m, err := c.OpenAvailable(ctx, account, hash)
	if err != nil {
		return nil, m, err
	}
	defer f.Close()
	if offset >= m.Size {
		return nil, m, errors.New("media offset exceeds file size")
	}
	b, err := io.ReadAll(io.NewSectionReader(f, offset, length))
	return b, m, err
}
