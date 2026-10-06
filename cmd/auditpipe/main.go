// auditpipe 是本地审计日志采集与转发管道的入口。
//
// 用法示例：
//
//	auditpipe -sink-dir ./out -checkpoint ./out/checkpoint \
//	    -file ./events.jsonl -http 127.0.0.1:8080 \
//	    -drop-prefix debug. -max-age 24h -max-bytes 104857600
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/checkpoint"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/pipeline"
	"github.com/feng111627/audit-log-pipeline-go/internal/retention"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	var files stringList
	var (
		httpAddr     = flag.String("http", "", "本地 HTTP 接入地址，如 127.0.0.1:8080（空则不启用）")
		sinkDir      = flag.String("sink-dir", "./audit-out", "转发目标目录")
		ckptPath     = flag.String("checkpoint", "", "位点文件路径（默认 <sink-dir>/checkpoint）")
		queueSize    = flag.Int("queue", 256, "有界队列容量（背压）")
		dropPrefix   = flag.String("drop-prefix", "", "丢弃 action 带此前缀的事件")
		defaultActor = flag.String("default-actor", "unknown", "补全缺失 actor 字段的默认值")
		maxAge       = flag.Duration("max-age", 0, "保留时限，超时清理（0 不限制）")
		maxBytes     = flag.Int64("max-bytes", 0, "保留容量上限，超出清理最旧分片（0 不限制）")
	)
	flag.Var(&files, "file", "本地 JSON Lines 事件文件（可多次指定）")
	flag.Parse()

	if *ckptPath == "" {
		*ckptPath = *sinkDir + "/checkpoint"
	}

	ds, err := sink.NewDirSink(*sinkDir)
	if err != nil {
		log.Fatalf("open sink: %v", err)
	}
	defer ds.Close()

	var sources []source.Source
	for _, f := range files {
		sources = append(sources, &source.FileSource{Path: f})
	}
	if *httpAddr != "" {
		sources = append(sources, &source.HTTPSource{Addr: *httpAddr})
	}
	if len(sources) == 0 {
		log.Fatal("at least one source required: -file or -http")
	}

	var rules []filter.Rule
	if *dropPrefix != "" {
		rules = append(rules, filter.Rule{ActionPrefix: *dropPrefix, Drop: true})
	}

	p := &pipeline.Pipeline{
		Sources:    sources,
		Filter:     &filter.Filter{Rules: rules, DefaultActor: *defaultActor},
		Sink:       ds,
		Checkpoint: checkpoint.NewStore(*ckptPath),
		QueueSize:  *queueSize,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 保留清理周期任务。
	if *maxAge > 0 || *maxBytes > 0 {
		go func() {
			tick := time.NewTicker(time.Minute)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if n, err := retention.Apply(*sinkDir, retention.Policy{MaxAge: *maxAge, MaxBytes: *maxBytes}, time.Now()); err != nil {
						log.Printf("retention: %v", err)
					} else if n > 0 {
						log.Printf("retention: reclaimed %d bytes", n)
					}
				}
			}
		}()
	}

	if err := p.Run(ctx); err != nil {
		log.Fatalf("pipeline: %v", err)
	}
	st := p.Stats()
	fmt.Printf("stats: received=%d accepted=%d filtered=%d rejected=%d delivered=%d retried=%d\n",
		st.Received, st.Accepted, st.Filtered, st.Rejected, st.Delivered, st.Retried)
	for _, r := range p.Rejections() {
		fmt.Printf("rejected: %s\n", r)
	}
}
