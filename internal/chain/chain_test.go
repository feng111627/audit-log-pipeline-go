package chain

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

func logCase(t *testing.T, biz, name, result string, extra ...string) {
	t.Helper()
	t.Logf("业务=%s 用例=%s 结果=%s %s", biz, name, result, strings.Join(extra, " "))
}

func writeRecords(t *testing.T, dir string, recs []Record) string {
	t.Helper()
	path := filepath.Join(dir, "seg-000000.log")
	var sb strings.Builder
	for _, r := range recs {
		b, _ := json.Marshal(r)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func makeChain(n int) []Record {
	c := NewChainer(1, GenesisHash)
	var recs []Record
	for i := 0; i < n; i++ {
		recs = append(recs, c.Append(event.Event{
			ID:        time.Now().Format("150405.000000") + "-" + string(rune('a'+i)),
			Timestamp: time.Now(),
			Action:    "login",
			Actor:     "alice",
		}))
	}
	return recs
}

func TestVerifyIntactChain(t *testing.T) {
	dir := t.TempDir()
	recs := makeChain(5)
	path := writeRecords(t, dir, recs)

	ok, reason, lastHash, count := VerifyFile(path, "")
	if !ok || count != 5 || lastHash != recs[4].Hash {
		logCase(t, "篡改检测", "完整链校验", "失败", "reason="+reason)
		t.Fatalf("ok=%v reason=%s count=%d", ok, reason, count)
	}
	logCase(t, "篡改检测", "完整链校验", "通过", "records=5")
}

func TestVerifyTamperedRecord(t *testing.T) {
	dir := t.TempDir()
	recs := makeChain(5)
	recs[2].Event.Actor = "mallory" // 篡改内容但不重算哈希
	path := writeRecords(t, dir, recs)

	ok, reason, _, _ := VerifyFile(path, "")
	if ok || !strings.Contains(reason, "tampered") {
		logCase(t, "篡改检测", "篡改记录内容", "失败", "reason="+reason)
		t.Fatalf("expected tamper detection, got ok=%v reason=%s", ok, reason)
	}
	logCase(t, "篡改检测", "篡改记录内容", "通过", "检出="+reason)
}

func TestVerifyBrokenLink(t *testing.T) {
	dir := t.TempDir()
	recs := makeChain(5)
	recs = append(recs[:2], recs[3:]...) // 抽掉一条
	for i := range recs {                // 重排序号以隔离出“链指针断裂”这一种错误
		recs[i].Seq = int64(i + 1)
	}
	path := writeRecords(t, dir, recs)

	ok, reason, _, _ := VerifyFile(path, "")
	if ok || !strings.Contains(reason, "broken link") {
		logCase(t, "篡改检测", "断链（抽掉记录）", "失败", "reason="+reason)
		t.Fatalf("expected broken link, got ok=%v reason=%s", ok, reason)
	}
	logCase(t, "篡改检测", "断链（抽掉记录）", "通过", "检出="+reason)
}

func TestVerifySeqGap(t *testing.T) {
	dir := t.TempDir()
	recs := makeChain(5)
	recs[3].Seq = 99 // 序号被改
	path := writeRecords(t, dir, recs)

	ok, reason, _, _ := VerifyFile(path, "")
	if ok {
		logCase(t, "篡改检测", "序号异常", "失败")
		t.Fatal("expected verification failure")
	}
	logCase(t, "篡改检测", "序号异常", "通过", "检出="+reason)
}
