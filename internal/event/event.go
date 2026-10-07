// Package event 定义审计事件模型、校验与字段补全。
package event

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// Event 是一条审计事件。
type Event struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Source    string            `json:"source"`
	Action    string            `json:"action"`
	Actor     string            `json:"actor"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// RejectReason 描述事件被拒绝的原因，要求一眼可区分。
type RejectReason string

const (
	RejectBadFormat    RejectReason = "BAD_FORMAT"    // 不是合法 JSON / 结构不符
	RejectMissingField RejectReason = "MISSING_FIELD" // 必填字段缺失
	RejectFiltered     RejectReason = "FILTERED"      // 被过滤规则丢弃
)

// Rejection 记录一条被拒绝的事件及其原因。
type Rejection struct {
	Reason RejectReason `json:"reason"`
	Detail string       `json:"detail"`
	Raw    string       `json:"raw"`
}

// Parse 从原始字节解析事件；失败时返回带原因的拒绝记录。
func Parse(raw []byte) (Event, *Rejection) {
	var e Event
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&e); err != nil {
		return Event{}, &Rejection{
			Reason: RejectBadFormat,
			Detail: fmt.Sprintf("invalid JSON: %v", err),
			Raw:    string(raw),
		}
	}
	return e, nil
}

// Validate 校验必填字段；缺失时返回拒绝记录。
func Validate(e Event) *Rejection {
	missing := []string{}
	if e.ID == "" {
		missing = append(missing, "id")
	}
	if e.Timestamp.IsZero() {
		missing = append(missing, "timestamp")
	}
	if e.Action == "" {
		missing = append(missing, "action")
	}
	if e.Actor == "" {
		missing = append(missing, "actor")
	}
	if len(missing) > 0 {
		return &Rejection{
			Reason: RejectMissingField,
			Detail: fmt.Sprintf("missing required fields: %v", missing),
			Raw:    marshalForLog(e),
		}
	}
	return nil
}

// Enrich 按规则补全字段：仅填充空字段，不覆盖已有值。
func Enrich(e Event, defaults map[string]string) Event {
	if e.Source == "" {
		e.Source = defaults["source"]
	}
	if e.Actor == "" {
		e.Actor = defaults["actor"]
	}
	if e.Fields == nil && defaults["fields"] != "" {
		e.Fields = map[string]string{"enriched": defaults["fields"]}
	}
	return e
}

func marshalForLog(e Event) string {
	b, err := json.Marshal(e)
	if err != nil {
		return "<unmarshalable>"
	}
	return string(b)
}
