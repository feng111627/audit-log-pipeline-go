// Package source 提供审计事件来源：本地文件尾随与本地 HTTP 接入。
package source

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// Source 是一个事件来源，向 out 推送解析成功的事件，
// 解析失败的输入通过 rej 上报拒绝原因。Read 阻塞直到 ctx 取消或来源耗尽。
type Source interface {
	Name() string
	Read(ctx context.Context, out chan<- *event.Event, rej chan<- *event.Rejection) error
}

// FileSource 按行读取本地文件（JSON Lines），文件被追加时持续尾随。
type FileSource struct {
	Path string
	// PollInterval 尾随轮询间隔，0 用默认值。
	PollInterval time.Duration
}

func (f *FileSource) Name() string { return "file:" + f.Path }

func (f *FileSource) Read(ctx context.Context, out chan<- *event.Event, rej chan<- *event.Rejection) error {
	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	poll := f.PollInterval
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if e := parseLine(line); e != nil {
				e.Source = f.Name()
				select {
				case out <- e:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else if r := rejectLine(line); r != nil {
				select {
				case rej <- r:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(poll):
				}
				continue
			}
			return err
		}
	}
}

// HTTPSource 在本地端口接收 POST 的 JSON 事件。
type HTTPSource struct {
	Addr string
	// MaxBody 限制单请求体大小，0 用默认值。
	MaxBody int64
}

func (h *HTTPSource) Name() string { return "http:" + h.Addr }

func (h *HTTPSource) Read(ctx context.Context, out chan<- *event.Event, rej chan<- *event.Rejection) error {
	maxBody := h.MaxBody
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		e, perr := event.Parse(body)
		if perr != nil {
			if rj, ok := perr.(*event.Rejection); ok {
				select {
				case rej <- rj:
				case <-r.Context().Done():
				}
				w.Header().Set("X-Reject-Reason", string(rj.Reason))
				http.Error(w, rj.Error(), http.StatusUnprocessableEntity)
				return
			}
			http.Error(w, perr.Error(), http.StatusBadRequest)
			return
		}
		e.Source = h.Name()
		select {
		case out <- e:
			w.WriteHeader(http.StatusAccepted)
		case <-r.Context().Done():
			http.Error(w, "pipeline unavailable", http.StatusServiceUnavailable)
		}
	})
	srv := &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", h.Addr)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}

func parseLine(line []byte) *event.Event {
	e, err := event.Parse(trimSpace(line))
	if err != nil {
		return nil
	}
	return e
}

func rejectLine(line []byte) *event.Rejection {
	trimmed := trimSpace(line)
	if len(trimmed) == 0 {
		return nil // 空行不算拒绝
	}
	_, err := event.Parse(trimmed)
	if rj, ok := err.(*event.Rejection); ok {
		return rj
	}
	return &event.Rejection{Reason: event.RejectMalformedJSON, Raw: string(trimmed)}
}

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r' || b[0] == '\n') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
