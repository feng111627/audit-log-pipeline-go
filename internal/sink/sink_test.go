package sink

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

func logCase(t *testing.T, biz, name string, pass bool) {
	t.Helper()
	result := "通过"
	if !pass {
		result = "失败"
	}
	t.Logf("业务=%s 用例=%s 结果=%s", biz, name, result)
}

func mkEvent(id string, seq uint64) *event.Event {
	return &event.Event{
		ID:        id,
		Action:    "login",
		Actor:     "alice",
		Timestamp: time.Date(2026, 10, 7, 0, 0, int(seq), 0, time.UTC),
		Seq:       seq,
	}
}

func TestDirSinkIdempotentWrite(t *testing.T) {
	biz := "投递目标"
	dir := t.TempDir()
	d, err := NewDirSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	ctx := context.Background()

	if err := d.Write(ctx, mkEvent("e1", 1)); err != nil {
		t.Fatal(err)
	}
	// 重复投递同一 Seq：幂等确认，不产生重复落库。
	if err := d.Write(ctx, mkEvent("e1", 1)); err != nil {
		t.Fatal(err)
	}
	seq, _ := d.LastSeq(ctx)
	events, _ := readSegment(filepath.Join(dir, "segment-000001.jsonl"))
	pass := seq == 1 && len(events) == 1
	logCase(t, biz, "重复投递幂等不重复落库", pass)
	if !pass {
		t.Fatalf("seq=%d events=%d", seq, len(events))
	}

	// 跳号：期望报错而不是从中间接着写。
	err = d.Write(ctx, mkEvent("e3", 3))
	pass = err != nil
	logCase(t, biz, "Seq跳号直接报错", pass)
	if !pass {
		t.Fatal("expected gap error")
	}
}

func TestDirSinkRestartRecovery(t *testing.T) {
	biz := "投递目标"
	dir := t.TempDir()
	ctx := context.Background()
	d, _ := NewDirSink(dir)
	for i := 1; i <= 3; i++ {
		if err := d.Write(ctx, mkEvent("e"+string(rune('0'+i)), uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	d.Close()

	// 模拟进程重启：重开目录，位点与链尾恢复。
	d2, err := NewDirSink(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d2.Close()
	seq, _ := d2.LastSeq(ctx)
	ids, _ := d2.KnownIDs(ctx)
	pass := seq == 3 && len(ids) == 3
	logCase(t, biz, "重启后位点与已知ID恢复", pass)
	if !pass {
		t.Fatalf("seq=%d ids=%v", seq, ids)
	}

	// 重启后续写，链延续。
	if err := d2.Write(ctx, mkEvent("e4", 4)); err != nil {
		t.Fatal(err)
	}
	bad, err := VerifyDir(dir)
	pass = err == nil && bad == -1
	logCase(t, biz, "重启续写链完整", pass)
	if !pass {
		t.Fatalf("verify bad=%d err=%v", bad, err)
	}
}

func TestDirSinkTamperDetection(t *testing.T) {
	biz := "投递目标"
	dir := t.TempDir()
	ctx := context.Background()
	d, _ := NewDirSink(dir)
	for i := 1; i <= 3; i++ {
		d.Write(ctx, mkEvent("e"+string(rune('0'+i)), uint64(i)))
	}
	d.Close()

	pass, err := VerifyDir(dir)
	if err != nil || pass != -1 {
		t.Fatalf("clean verify bad=%d err=%v", pass, err)
	}
	logCase(t, biz, "未篡改目录校验通过", true)

	// 篡改落盘文件中的一个字段。
	path := filepath.Join(dir, "segment-000001.jsonl")
	b, _ := os.ReadFile(path)
	tampered := []byte(string(b))
	for i := range tampered {
		if tampered[i] == 'a' && string(tampered[i:i+5]) == "alice" {
			tampered[i] = 'o' // alice -> olice
			break
		}
	}
	if err := os.WriteFile(path, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	bad, err := VerifyDir(dir)
	ok := err == nil && bad > 0
	logCase(t, biz, "落盘文件被篡改可检出", ok)
	if !ok {
		t.Fatalf("tamper not detected: bad=%d err=%v", bad, err)
	}
}
