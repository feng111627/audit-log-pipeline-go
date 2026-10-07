// Package source 实现本地文件与本地 HTTP 两类事件来源。
package source

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// Source 是一个事件来源，向 out 推送原始行，直到 ctx 取消。
// 写入 out 时阻塞等待，从而把下游背压传导到采集端。
type Source interface {
	Name() string
	Run(ctx context.Context, out chan<- []byte) error
}

// FileSource 从本地文件按行读取事件（模拟 tail：读完现有内容后继续跟随追加）。
type FileSource struct {
	Path         string
	PollInterval time.Duration // 跟随模式下的轮询间隔，默认 50ms
}

func (s *FileSource) Name() string { return "file:" + s.Path }

func (s *FileSource) Run(ctx context.Context, out chan<- []byte) error {
	f, err := os.Open(s.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	poll := s.PollInterval
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := trimNL(line)
			if len(trimmed) > 0 {
				select {
				case out <- trimmed:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		if err == io.EOF {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(poll):
			}
			continue
		}
		if err != nil {
			return err
		}
	}
}

// HTTPSource 通过本地 HTTP 接口接收事件（POST /ingest，body 为单行 JSON）。
type HTTPSource struct {
	Addr string
	ln   net.Listener
}

func (s *HTTPSource) Name() string { return "http:" + s.Addr }

func (s *HTTPSource) Run(ctx context.Context, out chan<- []byte) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		body = trimNL(body)
		if len(body) == 0 {
			http.Error(w, "empty body", http.StatusBadRequest)
			return
		}
		select {
		case out <- body:
			w.WriteHeader(http.StatusAccepted)
		case <-ctx.Done():
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
		}
	})
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	s.ln = ln
	srv := &http.Server{Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	err = srv.Serve(ln)
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// BoundAddr 返回实际监听地址（用于 :0 端口）。
func (s *HTTPSource) BoundAddr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func trimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
