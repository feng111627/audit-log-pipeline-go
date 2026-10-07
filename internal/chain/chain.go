// Package chain 对事件做防篡改哈希串联，并提供校验。
package chain

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/feng111627/audit-log-pipeline-go/internal/event"
)

// GenesisHash 是链的起点（第一条记录的 prev_hash）。
const GenesisHash = "GENESIS"

// Record 是带链指针的事件记录，逐行写入段文件。
type Record struct {
	Seq      int64       `json:"seq"`
	PrevHash string      `json:"prev_hash"`
	Hash     string      `json:"hash"`
	Event    event.Event `json:"event"`
}

// computeHash 由 seq、prev_hash 与事件内容计算记录哈希。
func computeHash(seq int64, prevHash string, e event.Event) string {
	payload, _ := json.Marshal(e)
	h := sha256.New()
	fmt.Fprintf(h, "%d|%s|", seq, prevHash)
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// Chainer 维护链尾状态，追加时计算哈希。
type Chainer struct {
	Seq      int64  // 下一条记录的序号
	LastHash string // 链尾哈希，空表示尚未初始化
}

// NewChainer 从已知链尾状态恢复（用于进程重启后续链）。
func NewChainer(nextSeq int64, lastHash string) *Chainer {
	if lastHash == "" {
		lastHash = GenesisHash
	}
	return &Chainer{Seq: nextSeq, LastHash: lastHash}
}

// Append 为事件生成下一条链记录。
func (c *Chainer) Append(e event.Event) Record {
	if c.LastHash == "" {
		c.LastHash = GenesisHash
	}
	r := Record{Seq: c.Seq, PrevHash: c.LastHash, Event: e}
	r.Hash = computeHash(r.Seq, r.PrevHash, e)
	c.Seq++
	c.LastHash = r.Hash
	return r
}

// VerifyFile 校验一个段文件内记录链是否完整、未被篡改。
// expectPrev 是该段第一条记录应有的 prev_hash（来自锚点或上一段）。
// ok 为 false 时 reason 描述问题；lastHash 与 count 描述链尾。
func VerifyFile(path string, expectPrev string) (ok bool, reason string, lastHash string, count int64) {
	f, err := os.Open(path)
	if err != nil {
		return false, fmt.Sprintf("open: %v", err), "", 0
	}
	defer f.Close()

	prev := expectPrev
	if prev == "" {
		prev = GenesisHash
	}
	var expectSeq int64 = -1
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			return false, fmt.Sprintf("line %d: corrupt record: %v", lineNo, err), prev, count
		}
		if expectSeq >= 0 && r.Seq != expectSeq {
			return false, fmt.Sprintf("line %d: seq gap: got %d want %d", lineNo, r.Seq, expectSeq), prev, count
		}
		if r.PrevHash != prev {
			return false, fmt.Sprintf("line %d: broken link: prev_hash mismatch", lineNo), prev, count
		}
		if want := computeHash(r.Seq, r.PrevHash, r.Event); want != r.Hash {
			return false, fmt.Sprintf("line %d: tampered record: hash mismatch", lineNo), prev, count
		}
		prev = r.Hash
		expectSeq = r.Seq + 1
		count++
	}
	if err := sc.Err(); err != nil {
		return false, fmt.Sprintf("read: %v", err), prev, count
	}
	return true, "", prev, count
}
