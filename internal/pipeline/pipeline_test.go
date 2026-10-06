package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/checkpoint"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

func logCase(t *testing.T, biz, name string, pass bool) {
	t.Helper()
	result := "通过"
	if !pass {
		result = "失败"
	}
	t.Logf("业务=%s 用例=%s 结果=%s", biz, name, result)
}

func writeEventsFile(t *testing.T, dir, name, prefix string, n int, extraLines ...string) string {
	t.Helper()
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, `{"id":"%s-%d","action":"user.login","timestamp":"2026-10-07T00:00:%02dZ","actor":"u%d"}`,
			prefix, i, i%60, i)
		b.WriteByte('\n')
	}
	for _, l := range extraLines {
		b.WriteString(l)
		b.WriteByte('\n')
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func countSinkEvents(t *testing.T, dir string) (int, map[string]int) {
	t.Helper()
	ids := map[string]int{}
	files, _ := filepath.Glob(filepath.Join(dir, "segment-*.jsonl"))
	total := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
			if line == "" {
				continue
			}
			var e event.Event
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatal(err)
			}
			ids[e.ID]++
			total++
		}
	}
	return total, ids
}

// flakySink 前 failTimes 次写入失败，模拟目标临时不可用。
type flakySink struct {
	inner     *sink.DirSink
	mu        sync.Mutex
	failTimes int
}

func (f *flakySink) Name() string { return "flaky" }
func (f *flakySink) Write(ctx context.Context, e *event.Event) error {
	f.mu.Lock()
	if f.failTimes > 0 {
		f.failTimes--
		f.mu.Unlock()
		return errors.New("target temporarily unavailable")
	}
	f.mu.Unlock()
	return f.inner.Write(ctx, e)
}
func (f *flakySink) LastSeq(ctx context.Context) (uint64, error) { return f.inner.LastSeq(ctx) }
func (f *flakySink) Close() error                                { return f.inner.Close() }
func (f *flakySink) KnownIDs(ctx context.Context) ([]string, error) {
	return f.inner.KnownIDs(ctx)
}

// slowSink 每次写入延迟，模拟目标变慢。
type slowSink struct {
	inner *sink.DirSink
	delay time.Duration
	gate  chan struct{} // 非 nil 时阻塞直到收到信号
}

func (s *slowSink) Name() string { return "slow" }
func (s *slowSink) Write(ctx context.Context, e *event.Event) error {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.inner.Write(ctx, e)
}
func (s *slowSink) LastSeq(ctx context.Context) (uint64, error) { return s.inner.LastSeq(ctx) }
func (s *slowSink) Close() error                                { return s.inner.Close() }
func (s *slowSink) KnownIDs(ctx context.Context) ([]string, error) {
	return s.inner.KnownIDs(ctx)
}

func newPipeline(dir string, sources []source.Source, sk sink.Sink) *Pipeline {
	return &Pipeline{
		Sources:       sources,
		Filter:        &filter.Filter{DefaultActor: "system"},
		Sink:          sk,
		Checkpoint:    checkpoint.NewStore(filepath.Join(dir, "checkpoint")),
		QueueSize:     16,
		RetryInterval: 5 * time.Millisecond,
	}
}

func TestConcurrentCollection(t *testing.T) {
	biz := "并发采集"
	dir := t.TempDir()
	// 3 个文件来源，每个 50 条；另有 1 个 HTTP 来源收 20 条。
	var sources []source.Source
	for i := 0; i < 3; i++ {
		p := writeEventsFile(t, dir, fmt.Sprintf("src%d.jsonl", i), fmt.Sprintf("f%d", i), 50)
		sources = append(sources, &source.FileSource{Path: p, PollInterval: 5 * time.Millisecond})
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	sources = append(sources, &source.HTTPSource{Addr: addr})

	ds, err := sink.NewDirSink(filepath.Join(dir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	p := newPipeline(dir, sources, ds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- p.Run(ctx) }()

	// 等 HTTP 监听就绪后 POST 20 条。
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 20; i++ {
		body := fmt.Sprintf(`{"id":"http-%d","action":"user.logout","timestamp":"2026-10-07T01:00:%02dZ"}`, i, i%60)
		resp, err := http.Post("http://"+addr+"/events", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("post %d: status %d", i, resp.StatusCode)
		}
	}

	waitFor(t, "170 events delivered", 10*time.Second, func() bool {
		return p.Stats().Delivered == 170
	})
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("run: %v", err)
	}

	total, ids := countSinkEvents(t, filepath.Join(dir, "out"))
	dups := 0
	for _, c := range ids {
		if c > 1 {
			dups++
		}
	}
	pass := total == 170 && dups == 0
	logCase(t, biz, "多源并发采集不重复不跟丢", pass)
	if !pass {
		t.Fatalf("total=%d dupIDs=%d", total, dups)
	}

	// Seq 连续性：1..170 无洞。
	seq, _ := ds.LastSeq(context.Background())
	pass = seq == 170
	logCase(t, biz, "序号连续无洞", pass)
	if !pass {
		t.Fatalf("lastSeq=%d want 170", seq)
	}
}

func TestRejectionReasons(t *testing.T) {
	biz := "并发采集"
	dir := t.TempDir()
	path := writeEventsFile(t, dir, "mixed.jsonl", "ok", 5,
		`not-json-at-all`,
		`{"action":"user.login","timestamp":"2026-10-07T00:00:00Z"}`,
		`{"id":"x1","action":"user.login"}`)
	ds, _ := sink.NewDirSink(filepath.Join(dir, "out"))
	defer ds.Close()
	p := newPipeline(dir, []source.Source{&source.FileSource{Path: path, PollInterval: 5 * time.Millisecond}}, ds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	waitFor(t, "5 delivered", 5*time.Second, func() bool { return p.Stats().Delivered == 5 })
	waitFor(t, "3 rejected", 5*time.Second, func() bool { return p.Stats().Rejected == 3 })
	cancel()

	reasons := map[event.RejectReason]bool{}
	for _, r := range p.Rejections() {
		reasons[r.Reason] = true
	}
	pass := reasons[event.RejectMalformedJSON] && reasons[event.RejectMissingField]
	logCase(t, biz, "坏事件带可区分拒绝原因", pass)
	if !pass {
		t.Fatalf("reasons=%v", reasons)
	}
	pass = p.Stats().Delivered == 5
	logCase(t, biz, "坏事件不影响好事件投递", pass)
	if !pass {
		t.Fatalf("delivered=%d", p.Stats().Delivered)
	}
}

func TestRestartRecovery(t *testing.T) {
	biz := "重启恢复"
	dir := t.TempDir()
	sinkDir := filepath.Join(dir, "out")
	srcPath := writeEventsFile(t, dir, "src.jsonl", "r", 30)

	run := func(expectDelivered uint64) {
		ds, err := sink.NewDirSink(sinkDir)
		if err != nil {
			t.Fatal(err)
		}
		defer ds.Close()
		p := newPipeline(dir, []source.Source{&source.FileSource{Path: srcPath, PollInterval: 5 * time.Millisecond}}, ds)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go p.Run(ctx)
		waitFor(t, "delivered", 5*time.Second, func() bool { return p.Stats().Delivered >= expectDelivered })
		cancel()
		time.Sleep(50 * time.Millisecond)
	}

	run(30) // 第一轮：30 条全部投递后取消（模拟进程退出）

	// 追加 10 条新事件，模拟重启后继续采集。
	f, _ := os.OpenFile(srcPath, os.O_APPEND|os.O_WRONLY, 0o644)
	for i := 30; i < 40; i++ {
		fmt.Fprintf(f, `{"id":"r-%d","action":"user.login","timestamp":"2026-10-07T02:00:%02dZ"}`+"\n", i, i%60)
	}
	f.Close()

	run(10) // 第二轮：重放的 30 条被去重，只新投递 10 条

	total, ids := countSinkEvents(t, sinkDir)
	dups := 0
	for _, c := range ids {
		if c > 1 {
			dups++
		}
	}
	pass := total == 40 && dups == 0
	logCase(t, biz, "重启后不丢不重", pass)
	if !pass {
		t.Fatalf("total=%d dups=%d", total, dups)
	}

	bad, err := sink.VerifyDir(sinkDir)
	pass = err == nil && bad == -1
	logCase(t, biz, "重启后链完整", pass)
	if !pass {
		t.Fatalf("verify bad=%d err=%v", bad, err)
	}
}

func TestSinkFailureRetry(t *testing.T) {
	biz := "目标故障"
	dir := t.TempDir()
	srcPath := writeEventsFile(t, dir, "src.jsonl", "fl", 20)
	ds, _ := sink.NewDirSink(filepath.Join(dir, "out"))
	defer ds.Close()
	fs := &flakySink{inner: ds, failTimes: 7}
	p := newPipeline(dir, []source.Source{&source.FileSource{Path: srcPath, PollInterval: 5 * time.Millisecond}}, fs)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	waitFor(t, "20 delivered", 5*time.Second, func() bool { return p.Stats().Delivered == 20 })
	cancel()
	time.Sleep(50 * time.Millisecond)

	st := p.Stats()
	pass := st.Retried >= 7 && st.Delivered == 20
	logCase(t, biz, "目标临时不可用重试后至少一次投递", pass)
	if !pass {
		t.Fatalf("retried=%d delivered=%d", st.Retried, st.Delivered)
	}
	total, ids := countSinkEvents(t, filepath.Join(dir, "out"))
	dups := 0
	for _, c := range ids {
		if c > 1 {
			dups++
		}
	}
	pass = total == 20 && dups == 0
	logCase(t, biz, "重试不产生重复落库", pass)
	if !pass {
		t.Fatalf("total=%d dups=%d", total, dups)
	}
}

func TestBackpressure(t *testing.T) {
	biz := "目标故障"
	dir := t.TempDir()
	srcPath := writeEventsFile(t, dir, "src.jsonl", "bp", 500)
	ds, _ := sink.NewDirSink(filepath.Join(dir, "out"))
	defer ds.Close()
	gate := make(chan struct{})
	ss := &slowSink{inner: ds, gate: gate}
	p := newPipeline(dir, []source.Source{&source.FileSource{Path: srcPath, PollInterval: 5 * time.Millisecond}}, ss)
	p.QueueSize = 8

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// 目标被闸住时，管道内积压必须受队列容量约束（背压），内存不被撑大。
	time.Sleep(300 * time.Millisecond)
	st := p.Stats()
	inflight := st.Accepted - st.Delivered
	// 上界 = 队列容量 + 投递中 1 条 + 调度中 1 条。
	pass := inflight <= uint64(p.QueueSize)+2
	logCase(t, biz, "目标变慢时积压受有界队列约束", pass)
	if !pass {
		t.Fatalf("inflight=%d queue=%d", inflight, p.QueueSize)
	}

	close(gate) // 放开闸门
	waitFor(t, "500 delivered", 10*time.Second, func() bool { return p.Stats().Delivered == 500 })
	cancel()
	pass = p.Stats().Delivered == 500
	logCase(t, biz, "恢复后全部投递不丢", pass)
	if !pass {
		t.Fatalf("delivered=%d", p.Stats().Delivered)
	}
}

func TestCheckpointRegression(t *testing.T) {
	biz := "重启恢复"
	dir := t.TempDir()
	// 位点文件写到 10，但目标为空：倒退必须报错。
	ck := checkpoint.NewStore(filepath.Join(dir, "checkpoint"))
	if err := ck.Save(10); err != nil {
		t.Fatal(err)
	}
	ds, _ := sink.NewDirSink(filepath.Join(dir, "out"))
	defer ds.Close()
	p := newPipeline(dir, nil, ds)
	err := p.Run(context.Background())
	pass := errors.Is(err, checkpoint.ErrRegression)
	logCase(t, biz, "位点倒退直接报错", pass)
	if !pass {
		t.Fatalf("err=%v", err)
	}
}

func TestFilterAndEnrich(t *testing.T) {
	biz := "过滤与补全"
	dir := t.TempDir()
	path := writeEventsFile(t, dir, "src.jsonl", "ft", 10,
		`{"id":"drop-1","action":"debug.ping","timestamp":"2026-10-07T00:00:00Z"}`,
		`{"id":"enrich-1","action":"user.login","timestamp":"2026-10-07T00:00:00Z"}`)
	ds, _ := sink.NewDirSink(filepath.Join(dir, "out"))
	defer ds.Close()
	p := newPipeline(dir, []source.Source{&source.FileSource{Path: path, PollInterval: 5 * time.Millisecond}}, ds)
	p.Filter = &filter.Filter{
		Rules:        []filter.Rule{{ActionPrefix: "debug.", Drop: true}},
		DefaultActor: "system",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	waitFor(t, "11 delivered", 5*time.Second, func() bool { return p.Stats().Delivered == 11 })
	cancel()
	time.Sleep(50 * time.Millisecond)

	st := p.Stats()
	pass := st.Filtered == 1 && st.Delivered == 11
	logCase(t, biz, "按规则丢弃debug事件", pass)
	if !pass {
		t.Fatalf("filtered=%d delivered=%d", st.Filtered, st.Delivered)
	}

	_, ids := countSinkEvents(t, filepath.Join(dir, "out"))
	_, dropped := ids["drop-1"]
	_, enriched := ids["enrich-1"]
	pass = !dropped && enriched
	logCase(t, biz, "缺actor事件补全后投递", pass)
	if !pass {
		t.Fatalf("dropped=%v enriched=%v", dropped, enriched)
	}
}
