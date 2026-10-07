// Package retention 按时间或容量清理旧段文件，且不影响链校验。
package retention

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
)

// Policy 定义保留规则。
type Policy struct {
	MaxAge   time.Duration // 段文件最大保留时长，0 表示不限
	MaxBytes int64         // 目录中段文件总容量上限，0 表示不限
}

// AnchorFile 记录被清理掉的链前缀的尾哈希，使剩余段的校验以它为起点，
// 清理本身不会导致链校验误报。
const AnchorFile = "ANCHOR"

type anchor struct {
	LastHash string `json:"last_hash"`
	Seq      int64  `json:"seq"`
}

// Apply 对 dir 下的段文件执行清理，返回被删除的段。
// 清理以整段为单位（绝不在段中间截断），并把最新被删段的链尾哈希写入
// ANCHOR 文件，保证剩余链校验不产生误报。最后一个段（活跃段）永不删除。
func Apply(dir string, p Policy, now time.Time) (deleted []string, err error) {
	segs, err := sink.ListSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(segs) <= 1 {
		return nil, nil // 保留活跃段
	}

	// 候选：除最后一个段以外的所有段。
	type segInfo struct {
		path    string
		size    int64
		modTime time.Time
	}
	var candidates []segInfo
	var total int64
	for _, s := range segs {
		fi, err := os.Stat(s)
		if err != nil {
			return nil, err
		}
		total += fi.Size()
		if s != segs[len(segs)-1] {
			candidates = append(candidates, segInfo{s, fi.Size(), fi.ModTime()})
		}
	}

	var toDelete []segInfo
	remaining := total
	for _, c := range candidates {
		ageExpired := p.MaxAge > 0 && now.Sub(c.modTime) > p.MaxAge
		sizeExceeded := p.MaxBytes > 0 && remaining > p.MaxBytes
		if ageExpired || sizeExceeded {
			toDelete = append(toDelete, c)
			remaining -= c.size
		}
	}

	// 逐个校验并删除：先读出被删段的链尾哈希作为新锚点。
	for _, c := range toDelete {
		ok, reason, lastHash, _ := chain.VerifyFile(c.path, anchorHash(dir))
		if !ok {
			// 段本身已被篡改/损坏：不删除，留给审计处理，并报告。
			return deleted, fmt.Errorf("refuse to prune %s: chain verification failed: %s", c.path, reason)
		}
		if err := os.Remove(c.path); err != nil {
			return deleted, err
		}
		if err := writeAnchor(dir, lastHash); err != nil {
			return deleted, err
		}
		deleted = append(deleted, c.path)
	}
	return deleted, nil
}

// anchorHash 读取当前锚点哈希；无锚点时返回空（表示从创世哈希开始）。
func anchorHash(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, AnchorFile))
	if err != nil {
		return ""
	}
	var a anchor
	if err := json.Unmarshal(b, &a); err != nil {
		return ""
	}
	return a.LastHash
}

func writeAnchor(dir, hash string) error {
	b, err := json.Marshal(anchor{LastHash: hash})
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, AnchorFile+".tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, AnchorFile))
}
