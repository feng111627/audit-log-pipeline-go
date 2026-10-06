// Package chain 提供事件防篡改哈希串联与校验。
package chain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// Genesis 是链首事件的前驱哈希。
const Genesis = "GENESIS"

// digest 计算事件内容哈希（不含 Hash 字段本身）。
func digest(e *event.Event) string {
	body := struct {
		ID        string            `json:"id"`
		Source    string            `json:"source"`
		Timestamp string            `json:"timestamp"`
		Action    string            `json:"action"`
		Actor     string            `json:"actor"`
		Labels    map[string]string `json:"labels,omitempty"`
		Payload   map[string]any    `json:"payload,omitempty"`
		Seq       uint64            `json:"seq"`
		PrevHash  string            `json:"prev_hash"`
	}{
		ID:        e.ID,
		Source:    e.Source,
		Timestamp: e.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		Action:    e.Action,
		Actor:     e.Actor,
		Labels:    e.Labels,
		Payload:   e.Payload,
		Seq:       e.Seq,
		PrevHash:  e.PrevHash,
	}
	b, _ := json.Marshal(body)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Seal 计算事件哈希并填入 PrevHash/Hash，prev 为上一条事件的哈希（链首用 Genesis）。
func Seal(e *event.Event, prev string) {
	if prev == "" {
		prev = Genesis
	}
	e.PrevHash = prev
	e.Hash = digest(e)
}

// Verify 校验一串事件（按 Seq 升序）的链完整性，
// 返回第一个断链/被篡改事件的下标，全部通过返回 -1。
// startPrev 为序列第一条事件应有的前驱哈希（已清理区间的锚点或 Genesis）。
func Verify(events []*event.Event, startPrev string) int {
	if startPrev == "" {
		startPrev = Genesis
	}
	prev := startPrev
	for i, e := range events {
		if e.PrevHash != prev {
			return i
		}
		if e.Hash != digest(e) {
			return i
		}
		prev = e.Hash
	}
	return -1
}

// Anchor 返回清理锚点说明，便于清单记录。
func Anchor(hash string) string {
	return fmt.Sprintf("anchored:%s", hash)
}
