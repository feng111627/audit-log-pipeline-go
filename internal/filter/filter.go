// Package filter 提供可配置的事件过滤规则。
package filter

import (
	"strings"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// Rule 是一条过滤规则：Action 前缀与 Actor 同时匹配（非空字段才参与）时事件被丢弃。
type Rule struct {
	ActionPrefix string `json:"action_prefix,omitempty"`
	Actor        string `json:"actor,omitempty"`
}

func (r Rule) matches(e event.Event) bool {
	if r.ActionPrefix != "" && !strings.HasPrefix(e.Action, r.ActionPrefix) {
		return false
	}
	if r.Actor != "" && e.Actor != r.Actor {
		return false
	}
	return r.ActionPrefix != "" || r.Actor != ""
}

// Filter 按规则集合过滤事件。
type Filter struct {
	Rules []Rule
}

// Match 报告事件是否命中某条规则（应被过滤）。
func (f *Filter) Match(e event.Event) (Rule, bool) {
	if f == nil {
		return Rule{}, false
	}
	for _, r := range f.Rules {
		if r.matches(e) {
			return r, true
		}
	}
	return Rule{}, false
}
