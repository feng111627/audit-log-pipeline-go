// Package sink 定义可插拔的转发目标及本地目录实现。
package sink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// Sink 是转发目标。Write 必须幂等：同一 Seq 的事件重复写入不得产生重复落库。
type Sink interface {
	Name() string
	// Write 写入一条事件；目标不可用时返回错误由上层重试。
	Write(ctx context.Context, e *event.Event) error
	// LastSeq 返回目标已持久化的最大连续 Seq，用于重启恢复与位点校验。
	LastSeq(ctx context.Context) (uint64, error)
	Close() error
}

// Segment 描述一个数据分片。
type Segment struct {
	Name      string `json:"name"`
	FirstSeq  uint64 `json:"first_seq"`
	LastSeq   uint64 `json:"last_seq"`
	FirstPrev string `json:"first_prev"` // 首条事件的 PrevHash，作为链锚点
	LastHash  string `json:"last_hash"`
	Deleted   bool   `json:"deleted,omitempty"`
}

// Manifest 记录分片清单，使保留清理后链校验可从锚点继续而不误报。
type Manifest struct {
	Segments []Segment `json:"segments"`
}

// DirSink 把事件以 JSON Lines 追加到本地目录下的分片数据文件，
// 并维护清单与位点，保证重启后不丢不重、链可校验。
type DirSink struct {
	Dir             string
	MaxSegmentBytes int64 // 单分片最大字节数，0 用默认值

	mu       sync.Mutex
	manifest Manifest
	file     *os.File
	curSize  int64
	lastSeq  uint64
	lastHash string
}

const defaultMaxSegmentBytes = 64 << 20

func NewDirSink(dir string) (*DirSink, error) {
	d := &DirSink{Dir: dir}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if err := d.loadManifest(); err != nil {
		return nil, err
	}
	// 恢复位点：以清单最后一个未删除分片的 LastSeq 为准。
	for i := len(d.manifest.Segments) - 1; i >= 0; i-- {
		seg := d.manifest.Segments[i]
		if !seg.Deleted {
			d.lastSeq = seg.LastSeq
			d.lastHash = seg.LastHash
			break
		}
	}
	// 打开当前分片追加写；无分片时按需创建。
	if n := len(d.manifest.Segments); n > 0 && !d.manifest.Segments[n-1].Deleted {
		if err := d.openSegment(d.manifest.Segments[n-1].Name); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *DirSink) Name() string { return "dir:" + d.Dir }

// Write 幂等写入：Seq 小于等于已持久化位点视为重复投递直接确认；
// Seq 跳号说明跟丢事件，直接报错。
func (d *DirSink) Write(ctx context.Context, e *event.Event) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.Seq <= d.lastSeq {
		return nil // 重复投递，幂等确认
	}
	if e.Seq != d.lastSeq+1 {
		return fmt.Errorf("seq gap: want %d got %d", d.lastSeq+1, e.Seq)
	}
	chain.Seal(e, d.lastHash)
	line, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if d.file == nil || (d.limit() > 0 && d.curSize+int64(len(line)+1) > d.limit()) {
		if err := d.rotate(); err != nil {
			return err
		}
	}
	if _, err := d.file.Write(append(line, '\n')); err != nil {
		return err
	}
	if err := d.file.Sync(); err != nil {
		return err
	}
	d.curSize += int64(len(line) + 1)
	d.lastSeq = e.Seq
	d.lastHash = e.Hash
	seg := &d.manifest.Segments[len(d.manifest.Segments)-1]
	seg.LastSeq = e.Seq
	seg.LastHash = e.Hash
	return d.saveManifestLocked()
}

func (d *DirSink) LastSeq(ctx context.Context) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.lastSeq, nil
}

func (d *DirSink) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}

// Manifest 返回清单副本，供保留清理与校验使用。
func (d *DirSink) Manifest() Manifest {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := Manifest{Segments: make([]Segment, len(d.manifest.Segments))}
	copy(cp.Segments, d.manifest.Segments)
	return cp
}

// RemoveSegment 将分片标记为已清理并删除文件（供 retention 调用）。
func (d *DirSink) RemoveSegment(name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.manifest.Segments {
		if d.manifest.Segments[i].Name == name && !d.manifest.Segments[i].Deleted {
			d.manifest.Segments[i].Deleted = true
			if err := os.Remove(filepath.Join(d.Dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			return d.saveManifestLocked()
		}
	}
	return fmt.Errorf("segment %q not found or already deleted", name)
}

func (d *DirSink) limit() int64 {
	if d.MaxSegmentBytes > 0 {
		return d.MaxSegmentBytes
	}
	return defaultMaxSegmentBytes
}

func (d *DirSink) rotate() error {
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			return err
		}
		d.file = nil
	}
	name := fmt.Sprintf("segment-%06d.jsonl", len(d.manifest.Segments)+1)
	d.manifest.Segments = append(d.manifest.Segments, Segment{
		Name:      name,
		FirstSeq:  d.lastSeq + 1,
		LastSeq:   d.lastSeq,
		FirstPrev: d.lastHashOrGenesis(),
	})
	if err := d.openSegment(name); err != nil {
		return err
	}
	return d.saveManifestLocked()
}

func (d *DirSink) openSegment(name string) error {
	path := filepath.Join(d.Dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	d.file = f
	d.curSize = st.Size()
	return nil
}

func (d *DirSink) lastHashOrGenesis() string {
	if d.lastHash == "" {
		return chain.Genesis
	}
	return d.lastHash
}

func (d *DirSink) manifestPath() string { return filepath.Join(d.Dir, "manifest.json") }

func (d *DirSink) loadManifest() error {
	b, err := os.ReadFile(d.manifestPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &d.manifest)
}

func (d *DirSink) saveManifestLocked() error {
	b, err := json.MarshalIndent(&d.manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp := d.manifestPath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, d.manifestPath())
}

// VerifyDir 校验目录中所有未删除分片的链完整性。
// 已清理分片通过清单锚点跳过，不报误报。返回断链事件序号，通过返回 -1。
func VerifyDir(dir string) (int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if errors.Is(err, os.ErrNotExist) {
		return -1, nil
	}
	if err != nil {
		return -1, err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return -1, err
	}
	sort.Slice(m.Segments, func(i, j int) bool { return m.Segments[i].Name < m.Segments[j].Name })
	for _, seg := range m.Segments {
		if seg.Deleted {
			continue
		}
		events, err := readSegment(filepath.Join(dir, seg.Name))
		if err != nil {
			return -1, err
		}
		if len(events) == 0 {
			continue
		}
		if idx := chain.Verify(events, seg.FirstPrev); idx >= 0 {
			return int64(events[idx].Seq), nil
		}
		// 跨分片连续性：当前分片首条 PrevHash 必须等于清单记录的锚点。
		if events[0].PrevHash != seg.FirstPrev {
			return int64(events[0].Seq), nil
		}
	}
	return -1, nil
}

func readSegment(path string) ([]*event.Event, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []*event.Event
	for _, line := range splitLines(b) {
		if len(line) == 0 {
			continue
		}
		var e event.Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("corrupt line in %s: %w", path, err)
		}
		out = append(out, &e)
	}
	return out, nil
}

func splitLines(b []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			lines = append(lines, b[start:i])
			start = i + 1
		}
	}
	if start < len(b) {
		lines = append(lines, b[start:])
	}
	return lines
}

// KnownIDs 返回所有未删除分片中已持久化事件的 ID，供管道重启后去重。
func (d *DirSink) KnownIDs(ctx context.Context) ([]string, error) {
	d.mu.Lock()
	segs := make([]Segment, len(d.manifest.Segments))
	copy(segs, d.manifest.Segments)
	d.mu.Unlock()
	var ids []string
	for _, seg := range segs {
		if seg.Deleted {
			continue
		}
		events, err := readSegment(filepath.Join(d.Dir, seg.Name))
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			ids = append(ids, e.ID)
		}
	}
	return ids, nil
}
