// Package event 定义审计事件模型、校验与拒绝原因。
package event

import (
	"encoding/json"
	"fmt"
	"time"
)

// RejectReason 是一眼可区分的事件拒绝原因。
type RejectReason string

const (
	RejectMalformedJSON RejectReason = "MALFORMED_JSON"
	RejectMissingField  RejectReason = "MISSING_FIELD"
	RejectInvalidField  RejectReason = "INVALID_FIELD"
)

// Rejection 描述一条被拒绝的原始输入及原因。
type Rejection struct {
	Reason RejectReason `json:"reason"`
	Field  string       `json:"field,omitempty"`
	Raw    string       `json:"raw"`
}

func (r *Rejection) Error() string {
	if r.Field != "" {
		return fmt.Sprintf("rejected(%s): field %q", r.Reason, r.Field)
	}
	return fmt.Sprintf("rejected(%s)", r.Reason)
}

// Event 是一条标准化后的审计事件。
type Event struct {
	ID        string            `json:"id"`
	Source    string            `json:"source"`
	Timestamp time.Time         `json:"timestamp"`
	Action    string            `json:"action"`
	Actor     string            `json:"actor"`
	Labels    map[string]string `json:"labels,omitempty"`
	Payload   map[string]any    `json:"payload,omitempty"`
	// Seq 由管道在入队时分配，用于去重与位点管理。
	Seq uint64 `json:"seq"`
	// PrevHash/Hash 由防篡改链在落库前填充。
	PrevHash string `json:"prev_hash,omitempty"`
	Hash     string `json:"hash,omitempty"`
}

// Parse 解析原始字节为 Event，失败时返回带分类原因的 *Rejection。
func Parse(raw []byte) (*Event, error) {
	var e Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, &Rejection{Reason: RejectMalformedJSON, Raw: truncate(string(raw))}
	}
	if err := e.Validate(); err != nil {
		if rej, ok := err.(*Rejection); ok {
			rej.Raw = truncate(string(raw))
		}
		return nil, err
	}
	return &e, nil
}

// Validate 校验必填字段，返回 nil 或 *Rejection。
func (e *Event) Validate() error {
	if e.ID == "" {
		return &Rejection{Reason: RejectMissingField, Field: "id"}
	}
	if e.Action == "" {
		return &Rejection{Reason: RejectMissingField, Field: "action"}
	}
	if e.Timestamp.IsZero() {
		return &Rejection{Reason: RejectMissingField, Field: "timestamp"}
	}
	if e.Timestamp.After(time.Now().Add(24 * time.Hour)) {
		return &Rejection{Reason: RejectInvalidField, Field: "timestamp"}
	}
	return nil
}

func truncate(s string) string {
	const max = 512
	if len(s) > max {
		return s[:max] + "...(truncated)"
	}
	return s
}
