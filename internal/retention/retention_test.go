package retention

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
)

func logCase(t *testing.T, biz, name string, pass bool) {
	t.Helper()
	result := "通过"
	if !pass {
		result = "失败"
	}
	t.Logf("业务=%s 用例=%s 结果=%s", biz, name, result)
}

// writeSegments 用很小的分片上限写入 n 条事件，制造多个分片。
func writeSegments(t *testing.T, dir string, n int) *sink.DirSink {
	t.Helper()
	d, err := sink.NewDirSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	d.MaxSegmentBytes = 200 // 强制频繁滚动分片
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		e := &event.Event{
			ID:        fmt.Sprintf("e%d", i),
			Action:    "login",
			Timestamp: time.Date(2026, 10, 7, 0, 0, i, 0, time.UTC),
			Seq:       uint64(i),
		}
		if err := d.Write(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func TestRetentionBySize(t *testing.T) {
	biz := "保留清理"
	dir := t.TempDir()
	d := writeSegments(t, dir, 20)
	m := d.Manifest()
	liveBefore := 0
	for _, s := range m.Segments {
		if !s.Deleted {
			liveBefore++
		}
	}
	d.Close()

	removed, err := Apply(dir, Policy{MaxBytes: 500}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	pass := removed > 0 && liveBefore > 2
	logCase(t, biz, "超容量清理最旧分片", pass)
	if !pass {
		t.Fatalf("removed=%d liveBefore=%d", removed, liveBefore)
	}

	// 清理后链校验不误报。
	bad, err := sink.VerifyDir(dir)
	pass = err == nil && bad == -1
	logCase(t, biz, "清理后链校验不误报", pass)
	if !pass {
		t.Fatalf("verify bad=%d err=%v", bad, err)
	}

	// 活跃分片必须保留。
	d2, _ := sink.NewDirSink(dir)
	defer d2.Close()
	seq, _ := d2.LastSeq(context.Background())
	pass = seq == 20
	logCase(t, biz, "活跃分片保留位点连续", pass)
	if !pass {
		t.Fatalf("lastSeq=%d want 20", seq)
	}
}

func TestRetentionByAge(t *testing.T) {
	biz := "保留清理"
	dir := t.TempDir()
	d := writeSegments(t, dir, 10)
	d.Close()

	// 把最旧分片的修改时间调到 48 小时前。
	m := sink.Manifest{}
	segs, _ := filepath.Glob(filepath.Join(dir, "segment-*.jsonl"))
	if len(segs) < 2 {
		t.Fatalf("want multiple segments, got %d", len(segs))
	}
	old := time.Now().Add(-48 * time.Hour)
	os.Chtimes(segs[0], old, old)
	_ = m

	removed, err := Apply(dir, Policy{MaxAge: 24 * time.Hour}, time.Now())
	pass := err == nil && removed > 0
	logCase(t, biz, "超时限清理过期分片", pass)
	if !pass {
		t.Fatalf("removed=%d err=%v", removed, err)
	}

	bad, err := sink.VerifyDir(dir)
	pass = err == nil && bad == -1
	logCase(t, biz, "按时间清理后链校验不误报", pass)
	if !pass {
		t.Fatalf("verify bad=%d err=%v", bad, err)
	}
}
