// Package checkpoint 负责投递位点的持久化与位点倒退检测。
package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrRegression 表示位点倒退，必须报错而不是从中间接着写。
var ErrRegression = errors.New("checkpoint regression: stored seq is ahead of sink")

// Store 持久化已确认投递的位点。
type Store struct {
	Path string
	mu   sync.Mutex
}

func NewStore(path string) *Store { return &Store{Path: path} }

// Load 读取已保存位点，无文件时返回 0。
func (s *Store) Load() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("corrupt checkpoint %s: %w", s.Path, err)
	}
	return n, nil
}

// Save 原子写入新位点；newSeq 小于已保存位点时返回 ErrRegression。
func (s *Store) Save(newSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, err := os.ReadFile(s.Path); err == nil {
		if cur, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64); err == nil && newSeq < cur {
			return fmt.Errorf("%w: stored=%d new=%d", ErrRegression, cur, newSeq)
		}
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return err
	}
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatUint(newSeq, 10)+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.Path)
}

// Verify 对比本地位点与目标位点，倒退时返回 ErrRegression。
func (s *Store) Verify(sinkSeq uint64) error {
	local, err := s.Load()
	if err != nil {
		return err
	}
	if sinkSeq < local {
		return fmt.Errorf("%w: local=%d sink=%d", ErrRegression, local, sinkSeq)
	}
	return nil
}
