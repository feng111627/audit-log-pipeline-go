package event

import (
	"testing"
	"time"
)

func logCase(t *testing.T, biz, name string, pass bool) {
	t.Helper()
	result := "通过"
	if !pass {
		result = "失败"
	}
	t.Logf("业务=%s 用例=%s 结果=%s", biz, name, result)
}

func TestParseRejects(t *testing.T) {
	biz := "事件解析与拒绝分类"
	cases := []struct {
		name string
		raw  string
		want RejectReason
	}{
		{"非JSON输入", `not-json`, RejectMalformedJSON},
		{"缺id", `{"action":"login","timestamp":"2026-10-07T00:00:00Z"}`, RejectMissingField},
		{"缺action", `{"id":"1","timestamp":"2026-10-07T00:00:00Z"}`, RejectMissingField},
		{"缺timestamp", `{"id":"1","action":"login"}`, RejectMissingField},
		{"timestamp超未来24h", `{"id":"1","action":"login","timestamp":"2999-01-01T00:00:00Z"}`, RejectInvalidField},
	}
	for _, c := range cases {
		_, err := Parse([]byte(c.raw))
		rj, ok := err.(*Rejection)
		pass := ok && rj.Reason == c.want
		if !pass {
			t.Errorf("case %s: got %v, want reason %s", c.name, err, c.want)
		}
		logCase(t, biz, c.name, pass)
	}
}

func TestParseValid(t *testing.T) {
	biz := "事件解析与拒绝分类"
	raw := `{"id":"e1","action":"login","timestamp":"2026-10-07T01:00:00Z","actor":"alice"}`
	e, err := Parse([]byte(raw))
	pass := err == nil && e.ID == "e1" && e.Action == "login" && e.Actor == "alice" &&
		e.Timestamp.Equal(time.Date(2026, 10, 7, 1, 0, 0, 0, time.UTC))
	if !pass {
		t.Fatalf("got %+v err=%v", e, err)
	}
	logCase(t, biz, "合法事件解析", pass)
}
