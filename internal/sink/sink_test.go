package sink

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

func logCase(t *testing.T, biz, name, result string, extra ...string) {
	t.Helper()
	t.Logf("业务=%s 用例=%s 结果=%s %s", biz, name, result, strings.Join(extra, " "))
}

func makeRecords(startSeq int64, n int, prevHash string) []chain.Record {
	c := chain.NewChainer(startSeq, prevHash)
	var recs []chain.Record
	for i := 0; i < n; i++ {
		recs = append(recs, c.Append(event.Event{
			ID:        "id-" + time.Now().Format("150405.000000") + "-" + string(rune('a'+i)),
			Timestamp: time.Now(),
			Action:    "write",
			Actor:     "bob",
		}))
	}
	return recs
}

func countLines(t *testing.T, dir string) int {
	t.Helper()
	segs, err := ListSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, s := range segs {
		ok, reason, _, n := chain.VerifyFile(s, prevFor(t, dir, s, segs))
		if !ok {
			t.Fatalf("segment %s invalid: %s", s, reason)
		}
		total += int(n)
	}
	return total
}

// prevFor 计算校验某段时应有的 prev_hash：首段用锚点/创世，其余用上一段尾哈希。
func prevFor(t *testing.T, dir, seg string, segs []string) string {
	t.Helper()
	if seg == segs[0] {
		return ""
	}
	prev := ""
	for _, s := range segs {
		if s == seg {
			return prev
		}
		ok, reason, lastHash, _ := chain.VerifyFile(s, prev)
		if !ok {
			t.Fatalf("segment %s invalid: %s", s, reason)
		}
		prev = lastHash
	}
	return ""
}

func TestSinkRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := OpenDirSink(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	batch1 := makeRecords(1, 10, chain.GenesisHash)
	if err := s1.WriteBatch(ctx, batch1); err != nil {
		t.Fatal(err)
	}
	_, lastHash := s1.State()

	// 模拟进程重启：重新打开同一目录。
	s2, err := OpenDirSink(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	committed, hash := s2.State()
	if committed != 10 || hash != lastHash {
		logCase(t, "重启恢复", "位点与链尾恢复", "失败")
		t.Fatalf("committed=%d want 10", committed)
	}

	// 重启后续写，链应延续。
	batch2 := makeRecords(11, 5, hash)
	if err := s2.WriteBatch(ctx, batch2); err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, dir); n != 15 {
		logCase(t, "重启恢复", "位点与链尾恢复", "失败")
		t.Fatalf("total records=%d want 15", n)
	}
	logCase(t, "重启恢复", "位点与链尾恢复", "通过", "重启前=10 重启后=15 链连续")
}

func TestSinkIdempotentRedelivery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := OpenDirSink(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	batch := makeRecords(1, 8, chain.GenesisHash)
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	// 目标故障恢复后重投同一批（至少一次语义的副作用），应幂等去重。
	if err := s.WriteBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(ctx, batch[:4]); err != nil {
		t.Fatal(err)
	}
	if n := countLines(t, dir); n != 8 {
		logCase(t, "目标故障", "重复投递幂等去重", "失败")
		t.Fatalf("records=%d want 8 (duplicates must be skipped)", n)
	}
	if s.Committed() != 8 {
		t.Fatalf("committed=%d want 8", s.Committed())
	}
	logCase(t, "目标故障", "重复投递幂等去重", "通过", "重投2次 落库=8")
}

func TestSinkOffsetRegression(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := OpenDirSink(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteBatch(ctx, makeRecords(1, 5, chain.GenesisHash)); err != nil {
		t.Fatal(err)
	}
	// 位点出现空洞：下一批从 seq=9 开始（应为 6），必须报错而不是接着写。
	err = s.WriteBatch(ctx, makeRecords(9, 3, "somehash"))
	if !errors.Is(err, ErrOffsetRegression) {
		logCase(t, "位点保护", "位点倒退/空洞报错", "失败")
		t.Fatalf("expected ErrOffsetRegression, got %v", err)
	}
	if n := countLines(t, dir); n != 5 {
		t.Fatalf("records=%d want 5 (nothing written after regression)", n)
	}
	logCase(t, "位点保护", "位点倒退/空洞报错", "通过", "err="+err.Error())
}
