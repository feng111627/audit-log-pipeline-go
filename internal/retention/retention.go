// Package retention 按时间或容量清理已落库的事件数据，且不影响链校验。
package retention

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
)

// Policy 定义保留规则：MaxAge 或 MaxBytes 为 0 表示该维度不限制。
type Policy struct {
	MaxAge   time.Duration
	MaxBytes int64
}

// Apply 对目录中的分片数据执行清理，返回删除的字节数。
// 清理以整分片为单位（永不清理最后一个活跃分片），
// 并在清单中保留锚点，使链校验跳过已清理区间而不误报。
func Apply(dir string, p Policy, now time.Time) (int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return 0, err
	}
	var m sink.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return 0, err
	}
	d, err := sink.NewDirSink(dir)
	if err != nil {
		return 0, err
	}
	defer d.Close()

	segs := m.Segments
	sort.Slice(segs, func(i, j int) bool { return segs[i].Name < segs[j].Name })

	// 统计未删除分片总字节。
	type segInfo struct {
		seg  sink.Segment
		size int64
	}
	var live []segInfo
	var total int64
	for _, s := range segs {
		if s.Deleted {
			continue
		}
		st, err := os.Stat(filepath.Join(dir, s.Name))
		if err != nil {
			continue
		}
		live = append(live, segInfo{s, st.Size()})
		total += st.Size()
	}

	var removed int64
	// 保留最后一个活跃分片，保证追加写不中断。
	for i := 0; i < len(live)-1; i++ {
		s := live[i]
		expired := p.MaxAge > 0 && now.Sub(fileModTime(dir, s.seg.Name)) > p.MaxAge
		oversize := p.MaxBytes > 0 && total-removed > p.MaxBytes
		if !expired && !oversize {
			continue
		}
		if err := d.RemoveSegment(s.seg.Name); err != nil {
			return removed, err
		}
		removed += s.size
	}
	return removed, nil
}

func fileModTime(dir, name string) time.Time {
	st, err := os.Stat(filepath.Join(dir, name))
	if err != nil {
		return time.Now()
	}
	return st.ModTime()
}
