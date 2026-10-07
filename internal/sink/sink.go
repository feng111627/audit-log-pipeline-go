// Package sink 定义可插拔转发目标及本地目录实现，保证至少一次且不重复落库。
package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
)

// ErrOffsetRegression 表示位点出现空洞/倒退，直接报错而不是从中间接着写。
var ErrOffsetRegression = errors.New("offset regression: incoming batch is not contiguous with committed offset")

// Sink 是可插拔的转发目标。
type Sink interface {
	Name() string
	// WriteBatch 原子地写入一批链记录；已提交过的记录须幂等去重。
	WriteBatch(ctx context.Context, batch []chain.Record) error
	// Committed 返回已提交的位点（最大连续 seq）。
	Committed() int64
}

// offsetState 持久化在 OFFSET 文件中。
type offsetState struct {
	Committed int64  `json:"committed"`
	LastHash  string `json:"last_hash"`
}

// DirSink 把链记录写入本地目录下的段文件，位点持久化在 OFFSET 文件。
type DirSink struct {
	Dir         string
	MaxSegBytes int64 // 段文件滚动阈值，<=0 用默认值

	mu        sync.Mutex
	committed int64
	lastHash  string
	segPath   string
	segSize   int64
}

const defaultMaxSegBytes = 4 << 20

// OpenDirSink 打开（必要时创建）一个目录目标，并从 OFFSET 恢复位点。
func OpenDirSink(dir string, maxSegBytes int64) (*DirSink, error) {
	if maxSegBytes <= 0 {
		maxSegBytes = defaultMaxSegBytes
	}
	s := &DirSink{Dir: dir, MaxSegBytes: maxSegBytes}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	st, err := s.loadOffset()
	if err != nil {
		return nil, err
	}
	s.committed, s.lastHash = st.Committed, st.LastHash
	seg, size, err := s.locateTailSegment()
	if err != nil {
		return nil, err
	}
	s.segPath, s.segSize = seg, size
	return s, nil
}

func (s *DirSink) Name() string { return "dir:" + s.Dir }

// Committed 返回已提交位点。
func (s *DirSink) Committed() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed
}

// State 返回位点与链尾哈希，供重启后续链。
func (s *DirSink) State() (committed int64, lastHash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed, s.lastHash
}

// WriteBatch 写入一批记录：
//   - seq <= committed 的记录视为重复投递，幂等跳过（保证不重复落库）；
//   - 第一条未提交记录的 seq 必须恰好是 committed+1，否则报 ErrOffsetRegression；
//   - 写入并 fsync 段文件后，原子更新 OFFSET，保证至少一次且不丢。
func (s *DirSink) WriteBatch(ctx context.Context, batch []chain.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var fresh []chain.Record
	for _, r := range batch {
		if r.Seq > s.committed {
			fresh = append(fresh, r)
		}
	}
	if len(fresh) == 0 {
		return nil // 整批都是重复投递
	}
	if fresh[0].Seq != s.committed+1 {
		return fmt.Errorf("%w: committed=%d batch starts at seq=%d",
			ErrOffsetRegression, s.committed, fresh[0].Seq)
	}
	for i := 1; i < len(fresh); i++ {
		if fresh[i].Seq != fresh[i-1].Seq+1 {
			return fmt.Errorf("%w: gap inside batch at seq=%d", ErrOffsetRegression, fresh[i].Seq)
		}
	}

	for _, r := range fresh {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.appendRecord(r); err != nil {
			return err
		}
		s.committed = r.Seq
		s.lastHash = r.Hash
	}
	return s.saveOffset()
}

// appendRecord 追加一条记录到当前段，必要时滚动段文件。
func (s *DirSink) appendRecord(r chain.Record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if s.segPath == "" || s.segSize+int64(len(line)) > s.MaxSegBytes {
		if err := s.rotate(); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(s.segPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	if _, err := w.Write(line); err != nil {
		f.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	s.segSize += int64(len(line))
	return nil
}

// rotate 滚动到下一个段文件。
func (s *DirSink) rotate() error {
	idx := 0
	if s.segPath != "" {
		idx = segIndex(s.segPath) + 1
	} else {
		segs, err := ListSegments(s.Dir)
		if err != nil {
			return err
		}
		if len(segs) > 0 {
			idx = segIndex(segs[len(segs)-1]) + 1
		}
	}
	s.segPath = filepath.Join(s.Dir, fmt.Sprintf("seg-%06d.log", idx))
	s.segSize = 0
	return nil
}

// loadOffset 读取 OFFSET 文件；不存在时返回零值。
func (s *DirSink) loadOffset() (offsetState, error) {
	var st offsetState
	b, err := os.ReadFile(filepath.Join(s.Dir, "OFFSET"))
	if errors.Is(err, os.ErrNotExist) {
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return st, fmt.Errorf("corrupt OFFSET file: %w", err)
	}
	return st, nil
}

// saveOffset 原子写 OFFSET 文件（tmp + rename + fsync）。
func (s *DirSink) saveOffset() error {
	st := offsetState{Committed: s.committed, LastHash: s.lastHash}
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.Dir, "OFFSET.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.Dir, "OFFSET")); err != nil {
		return err
	}
	d, err := os.Open(s.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// locateTailSegment 找到目录中最后一个段文件及其大小。
func (s *DirSink) locateTailSegment() (string, int64, error) {
	segs, err := ListSegments(s.Dir)
	if err != nil {
		return "", 0, err
	}
	if len(segs) == 0 {
		return "", 0, nil
	}
	last := segs[len(segs)-1]
	fi, err := os.Stat(last)
	if err != nil {
		return "", 0, err
	}
	return last, fi.Size(), nil
}

// ListSegments 按序号升序列出目录下所有段文件。
func ListSegments(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "seg-") && strings.HasSuffix(e.Name(), ".log") {
			segs = append(segs, filepath.Join(dir, e.Name()))
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segIndex(segs[i]) < segIndex(segs[j]) })
	return segs, nil
}

// segIndex 解析段文件序号。
func segIndex(path string) int {
	base := filepath.Base(path)
	n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(base, "seg-"), ".log"))
	return n
}
