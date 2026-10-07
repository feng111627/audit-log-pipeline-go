// auditpipe 是本地审计日志采集与转发管道的入口。
//
// 用法：
//
//	auditpipe run    -sink DIR [-file PATH ...] [-http ADDR] [选项]
//	auditpipe verify -sink DIR
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/pipeline"
	"github.com/feng111627/audit-log-pipeline-go/internal/retention"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "run":
		cmdRun(os.Args[2:])
	case "verify":
		cmdVerify(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  auditpipe run    -sink DIR [-file PATH ...] [-http ADDR] [-queue N] [-batch N]
                   [-max-age DUR] [-max-bytes N] [-drop-action-prefix P ...] [-drop-actor A ...]
  auditpipe verify -sink DIR`)
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var files multiFlag
	var dropPrefixes multiFlag
	var dropActors multiFlag
	sinkDir := fs.String("sink", "", "转发目标目录（必填）")
	httpAddr := fs.String("http", "", "本地 HTTP 接入地址，如 127.0.0.1:8080")
	queue := fs.Int("queue", 1024, "有界队列容量（背压）")
	batch := fs.Int("batch", 64, "每批投递记录数")
	maxAge := fs.Duration("max-age", 0, "段文件保留时长，0 不限")
	maxBytes := fs.Int64("max-bytes", 0, "段文件总容量上限，0 不限")
	fs.Var(&files, "file", "本地文件来源，可重复")
	fs.Var(&dropPrefixes, "drop-action-prefix", "过滤规则：丢弃 action 前缀，可重复")
	fs.Var(&dropActors, "drop-actor", "过滤规则：丢弃 actor，可重复")
	fs.Parse(args)

	if *sinkDir == "" {
		fmt.Fprintln(os.Stderr, "-sink is required")
		os.Exit(2)
	}

	var sources []source.Source
	for _, f := range files {
		sources = append(sources, &source.FileSource{Path: f})
	}
	if *httpAddr != "" {
		sources = append(sources, &source.HTTPSource{Addr: *httpAddr})
	}
	if len(sources) == 0 {
		fmt.Fprintln(os.Stderr, "at least one source (-file or -http) is required")
		os.Exit(2)
	}

	var rules []filter.Rule
	for _, p := range dropPrefixes {
		rules = append(rules, filter.Rule{ActionPrefix: p})
	}
	for _, a := range dropActors {
		rules = append(rules, filter.Rule{Actor: a})
	}

	sk, err := sink.OpenDirSink(*sinkDir, 0)
	if err != nil {
		log.Fatalf("open sink: %v", err)
	}
	logger := log.New(os.Stderr, "[auditpipe] ", log.LstdFlags)
	p := pipeline.New(pipeline.Config{
		QueueSize:  *queue,
		BatchSize:  *batch,
		EnrichWith: map[string]string{"source": "auditpipe"},
		Logger:     logger,
	}, sources, &filter.Filter{Rules: rules}, sk)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 定期执行保留清理。
	if *maxAge > 0 || *maxBytes > 0 {
		go func() {
			tick := time.NewTicker(time.Minute)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-tick.C:
					deleted, err := retention.Apply(*sinkDir, retention.Policy{MaxAge: *maxAge, MaxBytes: *maxBytes}, t)
					if err != nil {
						logger.Printf("retention: %v", err)
					} else if len(deleted) > 0 {
						logger.Printf("retention: pruned %d segment(s)", len(deleted))
					}
				}
			}
		}()
	}

	logger.Printf("pipeline started: %d source(s) -> %s", len(sources), sk.Name())
	p.Run(ctx)
	ing, del, rej, dup := p.Stats()
	logger.Printf("stopped: ingested=%d delivered=%d rejected=%d duplicates=%d", ing, del, rej, dup)
	for _, r := range p.Rejections() {
		logger.Printf("rejection [%s] %s raw=%q", r.Reason, r.Detail, r.Raw)
	}
}

func cmdVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	sinkDir := fs.String("sink", "", "转发目标目录（必填）")
	fs.Parse(args)
	if *sinkDir == "" {
		fmt.Fprintln(os.Stderr, "-sink is required")
		os.Exit(2)
	}
	segs, err := sink.ListSegments(*sinkDir)
	if err != nil {
		log.Fatalf("list segments: %v", err)
	}
	prev := readAnchor(*sinkDir)
	var total int64
	for _, seg := range segs {
		ok, reason, lastHash, count := chain.VerifyFile(seg, prev)
		if !ok {
			fmt.Printf("FAIL %s: %s\n", seg, reason)
			os.Exit(1)
		}
		fmt.Printf("OK   %s: %d record(s)\n", seg, count)
		prev = lastHash
		total += count
	}
	fmt.Printf("chain verified: %d segment(s), %d record(s)\n", len(segs), total)
}

func readAnchor(dir string) string {
	b, err := os.ReadFile(dir + "/ANCHOR")
	if err != nil {
		return ""
	}
	var a struct {
		LastHash string `json:"last_hash"`
	}
	if err := json.Unmarshal(b, &a); err != nil {
		return ""
	}
	return a.LastHash
}
