package retention

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
)

func logCase(t *testing.T, biz, name, result string, extra ...string) {
	t.Helper()
	t.Logf("业务=%s 用例=%s 结果=%s %s", biz, name, result, strings.Join(extra, " "))
}

// fillSink 写入 n 条事件，小段以便滚动出多个段文件。
func fillSink(t *testing.T, dir string, n int) {
	t.Helper()
	sk, err := sink.OpenDirSink(dir, 512) // 很小的段，强制滚动
	if err != nil {
		t.Fatal(err)
	}
	c := chain.NewChainer(1, chain.GenesisHash)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		rec := c.Append(event.Event{
			ID:        fmt.Sprintf("ret-%04d", i),
			Timestamp: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
			Action:    "write",
			Actor:     "alice",
		})
		if err := sk.WriteBatch(ctx, []chain.Record{rec}); err != nil {
			t.Fatal(err)
		}
	}
}

// verifyRemaining 从锚点开始校验目录中所有剩余段。
func verifyRemaining(t *testing.T, dir string) (int, int64) {
	t.Helper()
	segs, err := sink.ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	prev := anchorHash(dir)
	var total int64
	for _, s := range segs {
		ok, reason, lastHash, n := chain.VerifyFile(s, prev)
		if !ok {
			t.Fatalf("segment %s failed verification after pruning: %s", s, reason)
		}
		prev = lastHash
		total += n
	}
	return len(segs), total
}

func TestPruneBySizeKeepsChainVerifiable(t *testing.T) {
	dir := t.TempDir()
	fillSink(t, dir, 40)

	segsBefore, _ := sink.ListSegments(dir)
	if len(segsBefore) < 3 {
		t.Fatalf("expected multiple segments, got %d", len(segsBefore))
	}
	deleted, err := Apply(dir, Policy{MaxBytes: 1024}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) == 0 {
		logCase(t, "保留清理", "按容量整段清理", "失败", "未删除任何段")
		t.Fatal("expected some segments deleted")
	}
	// 清理后剩余链必须校验通过：不产生误报。
	nSeg, nRec := verifyRemaining(t, dir)
	// 活跃段必须保留。
	segsAfter, _ := sink.ListSegments(dir)
	if segsAfter[len(segsAfter)-1] != segsBefore[len(segsBefore)-1] {
		t.Fatal("active segment must not be pruned")
	}
	logCase(t, "保留清理", "按容量整段清理", "通过",
		fmt.Sprintf("清理前=%d段 删除=%d段 剩余=%d段/%d条 校验=OK(无误报)", len(segsBefore), len(deleted), nSeg, nRec))
}

func TestPruneByAge(t *testing.T) {
	dir := t.TempDir()
	fillSink(t, dir, 30)
	// 把除活跃段外的段修改为 2 小时前。
	segs, _ := sink.ListSegments(dir)
	old := time.Now().Add(-2 * time.Hour)
	for _, s := range segs[:len(segs)-1] {
		if err := os.Chtimes(s, old, old); err != nil {
			t.Fatal(err)
		}
	}
	deleted, err := Apply(dir, Policy{MaxAge: time.Hour}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != len(segs)-1 {
		logCase(t, "保留清理", "按时间清理过期段", "失败")
		t.Fatalf("deleted=%d want %d", len(deleted), len(segs)-1)
	}
	nSeg, nRec := verifyRemaining(t, dir)
	logCase(t, "保留清理", "按时间清理过期段", "通过",
		fmt.Sprintf("删除=%d段 剩余=%d段/%d条 校验=OK(无误报)", len(deleted), nSeg, nRec))
}

func TestPruneRefusesTamperedSegment(t *testing.T) {
	dir := t.TempDir()
	fillSink(t, dir, 30)
	segs, _ := sink.ListSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("need >=2 segments, got %d", len(segs))
	}
	// 篡改第一个段的内容。
	victim := segs[0]
	b, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	b[10] ^= 0xFF
	if err := os.WriteFile(victim, b, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = Apply(dir, Policy{MaxBytes: 1}, time.Now()) // 容量压到极小，触发清理
	if err == nil || !strings.Contains(err.Error(), "verification failed") {
		logCase(t, "保留清理", "被篡改段拒绝清理", "失败")
		t.Fatalf("expected refusal, got %v", err)
	}
	// 被篡改的段必须还在，留给审计。
	if _, statErr := os.Stat(victim); statErr != nil {
		t.Fatal("tampered segment must be retained for audit")
	}
	logCase(t, "保留清理", "被篡改段拒绝清理", "通过", "err="+err.Error())
}
