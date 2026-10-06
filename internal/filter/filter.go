// Package filter 提供可配置的事件过滤与字段补全。
package filter

import (
	"strings"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// Rule 描述一条过滤规则：匹配 Action 前缀（为空表示全部匹配）。
type Rule struct {
	ActionPrefix string `json:"action_prefix"`
	Drop         bool   `json:"drop"`
}

// Filter 按规则过滤事件，并可补全默认字段。
type Filter struct {
	Rules        []Rule
	DefaultActor string
}

// Decide 返回 true 表示保留事件。规则按顺序匹配，命中即定；无命中默认保留。
func (f *Filter) Decide(e *event.Event) bool {
	if f == nil {
		return true
	}
	for _, r := range f.Rules {
		if r.ActionPrefix == "" || strings.HasPrefix(e.Action, r.ActionPrefix) {
			return !r.Drop
		}
	}
	return true
}

// Enrich 补全缺失字段（如默认 actor）。
func (f *Filter) Enrich(e *event.Event) {
	if f == nil {
		return
	}
	if e.Actor == "" && f.DefaultActor != "" {
		e.Actor = f.DefaultActor
	}
	if e.Labels == nil {
		e.Labels = map[string]string{}
	}
}
