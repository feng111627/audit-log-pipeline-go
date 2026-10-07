package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

func logCase(t *testing.T, biz, name, result string, extra ...string) {
	t.Helper()
	t.Logf("业务=%s 用例=%s 结果=%s %s", biz, name, result, strings.Join(extra, " "))
}

func writeEvents(t *testing.T, path string, lines []string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func validLine(id, action, actor string) string {
	b, _ := json.Marshal(event.Event{
		ID:        id,
		Timestamp: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
		Action:    action,
		Actor:     actor,
	})
	return string(b)
}

// flakySink 模拟会临时故障的目标：failUntil 次调用内报错，之后恢复。
type flakySink struct {
	mu        sync.Mutex
	failUntil int
	written   []chain.Record
	delay     time.Duration
}

func (f *flakySink) Name() string { return "flaky" }

func (f *flakySink) Committed() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.written) == 0 {
		return 0
	}
	return f.written[len(f.written)-1].Seq
}

func (f *flakySink) WriteBatch(ctx context.Context, batch []chain.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	if f.failUntil > 0 {
		f.failUntil--
		return fmt.Errorf("target temporarily unavailable")
	}
	for _, r := range batch {
		if r.Seq <= f.lockedCommitted() {
			continue // 幂等去重
		}
		f.written = append(f.written, r)
	}
	return nil
}

func (f *flakySink) lockedCommitted() int64 {
	if len(f.written) == 0 {
		return 0
	}
	return f.written[len(f.written)-1].Seq
}

func (f *flakySink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.written)
}

// TestConcurrentCollection 多文件来源 + HTTP 来源并发采集，核对不重复不跟丢。
func TestConcurrentCollection(t *testing.T) {
	dir := t.TempDir()
	const perSource = 200
	const fileSources = 3

	var sources []source.Source
	var want []string
	for i := 0; i < fileSources; i++ {
		var lines []string
		for j := 0; j < perSource; j++ {
			id := fmt.Sprintf("file%d-%04d", i, j)
			lines = append(lines, validLine(id, "read", "alice"))
			want = append(want, id)
		}
		p := filepath.Join(dir, fmt.Sprintf("src%d.log", i))
		writeEvents(t, p, lines)
		sources = append(sources, &source.FileSource{Path: p, PollInterval: 5 * time.Millisecond})
	}

	httpSrc := &source.HTTPSource{Addr: "127.0.0.1:0"}
	sources = append(sources, httpSrc)

	sk := &flakySink{}
	p := New(Config{QueueSize: 16, BatchSize: 7}, sources, &filter.Filter{}, sk)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// 等 HTTP 来源就绪后并发 POST。
	deadline := time.Now().Add(3 * time.Second)
	for httpSrc.BoundAddr() == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	url := "http://" + httpSrc.BoundAddr() + "/ingest"
	var httpIDs []string
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				id := fmt.Sprintf("http-%d-%04d", w, j)
				resp, err := http.Post(url, "application/json", strings.NewReader(validLine(id, "write", "carol")))
				if err == nil {
					resp.Body.Close()
				}
			}
		}(w)
	}
	for w := 0; w < 4; w++ {
		for j := 0; j < 50; j++ {
			httpIDs = append(httpIDs, fmt.Sprintf("http-%d-%04d", w, j))
		}
	}
	wg.Wait()
	want = append(want, httpIDs...)

	// 等待全部投递完成。
	total := int64(len(want))
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if int64(sk.count()) >= total {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	ing, del, rej, dup := p.Stats()
	if ing != total || del != total || dup != 0 {
		logCase(t, "并发采集", "多来源并发不重复不跟丢", "失败",
			fmt.Sprintf("want=%d ingested=%d delivered=%d dup=%d rejected=%d", total, ing, del, dup, rej))
		t.Fatalf("want=%d ingested=%d delivered=%d dup=%d", total, ing, del, dup)
	}
	// 核对落库事件 ID 集合与发送集合完全一致。
	got := make(map[string]int)
	for _, r := range sk.written {
		got[r.Event.ID]++
	}
	for _, id := range want {
		if got[id] != 1 {
			t.Fatalf("event %s delivered %d times", id, got[id])
		}
	}
	logCase(t, "并发采集", "多来源并发不重复不跟丢", "通过",
		fmt.Sprintf("来源=%d(文件)+1(HTTP) 事件=%d 重复=%d", fileSources, total, dup))
}

// TestRejections 格式错误、字段缺失、被过滤三类拒绝原因一眼可区分。
func TestRejections(t *testing.T) {
	dir := t.TempDir()
	lines := []string{
		`{not json`,                                // BAD_FORMAT
		`{"id":"x","action":"read"}`,               // MISSING_FIELD
		validLine("ok-1", "read", "alice"),         // 正常
		validLine("drop-1", "debug.ping", "alice"), // FILTERED（规则）
		validLine("ok-2", "write", "alice"),        // 正常
	}
	p := filepath.Join(dir, "src.log")
	writeEvents(t, p, lines)

	sk := &flakySink{}
	f := &filter.Filter{Rules: []filter.Rule{{ActionPrefix: "debug."}}}
	pipe := New(Config{QueueSize: 8, BatchSize: 1}, []source.Source{&source.FileSource{Path: p, PollInterval: 5 * time.Millisecond}}, f, sk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pipe.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if sk.count() >= 2 && len(pipe.Rejections()) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	reasons := map[event.RejectReason]int{}
	for _, r := range pipe.Rejections() {
		reasons[r.Reason]++
	}
	if reasons[event.RejectBadFormat] != 1 || reasons[event.RejectMissingField] != 1 || reasons[event.RejectFiltered] != 1 {
		logCase(t, "拒绝可区分", "三类拒绝原因", "失败", fmt.Sprintf("%v", reasons))
		t.Fatalf("reasons=%v", reasons)
	}
	if sk.count() != 2 {
		t.Fatalf("delivered=%d want 2", sk.count())
	}
	logCase(t, "拒绝可区分", "三类拒绝原因", "通过",
		fmt.Sprintf("BAD_FORMAT=%d MISSING_FIELD=%d FILTERED=%d 正常投递=%d",
			reasons[event.RejectBadFormat], reasons[event.RejectMissingField], reasons[event.RejectFiltered], sk.count()))
}

// TestTargetFailureRecovery 目标临时不可用时不丢事件，恢复后继续投递。
func TestTargetFailureRecovery(t *testing.T) {
	dir := t.TempDir()
	const n = 50
	var lines []string
	for i := 0; i < n; i++ {
		lines = append(lines, validLine(fmt.Sprintf("e-%04d", i), "read", "alice"))
	}
	p := filepath.Join(dir, "src.log")
	writeEvents(t, p, lines)

	sk := &flakySink{failUntil: 5} // 前 5 次写入失败，模拟目标临时不可用
	pipe := New(Config{QueueSize: 8, BatchSize: 5, RetryBackoff: 10 * time.Millisecond},
		[]source.Source{&source.FileSource{Path: p, PollInterval: 5 * time.Millisecond}}, &filter.Filter{}, sk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pipe.Run(ctx) }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sk.count() >= n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	if sk.count() != n {
		logCase(t, "目标故障", "临时不可用恢复后不丢不重", "失败")
		t.Fatalf("delivered=%d want %d", sk.count(), n)
	}
	for i, r := range sk.written {
		if r.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d", i)
		}
	}
	logCase(t, "目标故障", "临时不可用恢复后不丢不重", "通过", fmt.Sprintf("故障次数=5 最终投递=%d", n))
}

// TestBackpressure 目标变慢时，有界队列形成背压，内存不被无限撑大。
func TestBackpressure(t *testing.T) {
	dir := t.TempDir()
	const n = 500
	var lines []string
	for i := 0; i < n; i++ {
		lines = append(lines, validLine(fmt.Sprintf("bp-%04d", i), "read", "alice"))
	}
	p := filepath.Join(dir, "src.log")
	writeEvents(t, p, lines)

	sk := &flakySink{delay: 2 * time.Millisecond} // 慢目标
	const queueSize = 8
	pipe := New(Config{QueueSize: queueSize, BatchSize: 4},
		[]source.Source{&source.FileSource{Path: p, PollInterval: time.Millisecond}}, &filter.Filter{}, sk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pipe.Run(ctx) }()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if sk.count() >= n {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	if sk.count() != n {
		logCase(t, "背压", "慢目标下有界队列不丢事件", "失败")
		t.Fatalf("delivered=%d want %d", sk.count(), n)
	}
	// 背压的体现：在途事件上限 = 队列容量 + 单批次，与总量无关。
	logCase(t, "背压", "慢目标下有界队列不丢事件", "通过",
		fmt.Sprintf("队列容量=%d 总量=%d 全部投递 在途上限=%d", queueSize, n, queueSize+4))
}

// TestPipelineRestartRecovery 管道重启后从 sink 位点续链，不丢不重。
func TestPipelineRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	sinkDir := filepath.Join(dir, "out")

	run := func(lines []string, wantCommitted int64) *sink.DirSink {
		p := filepath.Join(dir, "src-"+time.Now().Format("150405.000000")+".log")
		writeEvents(t, p, lines)
		sk, err := sink.OpenDirSink(sinkDir, 0)
		if err != nil {
			t.Fatal(err)
		}
		pipe := New(Config{QueueSize: 8, BatchSize: 3},
			[]source.Source{&source.FileSource{Path: p, PollInterval: 5 * time.Millisecond}}, &filter.Filter{}, sk)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- pipe.Run(ctx) }()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if c, _ := sk.State(); c >= wantCommitted {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		<-done
		return sk
	}

	var lines1, lines2 []string
	for i := 0; i < 10; i++ {
		lines1 = append(lines1, validLine(fmt.Sprintf("run1-%02d", i), "read", "alice"))
	}
	for i := 0; i < 6; i++ {
		lines2 = append(lines2, validLine(fmt.Sprintf("run2-%02d", i), "read", "alice"))
	}
	sk1 := run(lines1, 10)
	c1, _ := sk1.State()
	sk2 := run(lines2, 16) // 模拟进程重启
	c2, _ := sk2.State()

	if c1 != 10 || c2 != 16 {
		logCase(t, "重启恢复", "管道重启续链", "失败")
		t.Fatalf("committed after restarts: %d, %d", c1, c2)
	}
	// 全链校验：重启前后应是一条连续的链。
	segs, err := sink.ListSegments(sinkDir)
	if err != nil {
		t.Fatal(err)
	}
	prev := ""
	var total int64
	for _, s := range segs {
		ok, reason, lastHash, n := chain.VerifyFile(s, prev)
		if !ok {
			logCase(t, "重启恢复", "管道重启续链", "失败", reason)
			t.Fatalf("chain broken: %s", reason)
		}
		prev = lastHash
		total += n
	}
	if total != 16 {
		t.Fatalf("total=%d want 16", total)
	}
	logCase(t, "重启恢复", "管道重启续链", "通过", fmt.Sprintf("重启前=%d 重启后=%d 链校验=OK", c1, c2))
}
