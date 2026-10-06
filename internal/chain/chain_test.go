package chain

import (
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

func mkEvents(t *testing.T, n int) []*event.Event {
	t.Helper()
	var out []*event.Event
	prev := Genesis
	for i := 0; i < n; i++ {
		e := &event.Event{
			ID:        string(rune('a' + i)),
			Action:    "login",
			Timestamp: time.Date(2026, 10, 7, 0, 0, i, 0, time.UTC),
			Seq:       uint64(i + 1),
		}
		Seal(e, prev)
		prev = e.Hash
		out = append(out, e)
	}
	return out
}

func TestChainIntact(t *testing.T) {
	biz := "防篡改串联"
	events := mkEvents(t, 5)
	pass := Verify(events, Genesis) == -1
	if !pass {
		t.Fatal("intact chain failed verification")
	}
	logCase(t, biz, "完整链校验通过", pass)
}

func TestChainTampered(t *testing.T) {
	biz := "防篡改串联"
	events := mkEvents(t, 5)
	events[2].Actor = "mallory" // 篡改内容
	pass := Verify(events, Genesis) == 2
	logCase(t, biz, "篡改事件内容被检出", pass)
	if !pass {
		t.Fatal("tampering not detected")
	}
}

func TestChainBroken(t *testing.T) {
	biz := "防篡改串联"
	events := mkEvents(t, 5)
	// 断链：抽掉中间一条，重新拼接但 PrevHash 对不上。
	broken := append([]*event.Event{}, events[:2]...)
	broken = append(broken, events[3:]...)
	pass := Verify(broken, Genesis) == 2
	logCase(t, biz, "断链被检出", pass)
	if !pass {
		t.Fatal("broken chain not detected")
	}
}

func TestChainAnchorAfterRetention(t *testing.T) {
	biz := "防篡改串联"
	events := mkEvents(t, 5)
	// 模拟前 3 条被保留清理，从锚点（第3条哈希）继续校验。
	rest := events[3:]
	pass := Verify(rest, events[2].Hash) == -1
	logCase(t, biz, "清理后锚点校验不误报", pass)
	if !pass {
		t.Fatal("anchor verification false positive")
	}
}
