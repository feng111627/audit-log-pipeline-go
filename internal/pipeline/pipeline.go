// Package pipeline 编排采集、过滤、投递与位点提交，提供背压与去重。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/checkpoint"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

// Stats 汇总管道运行计数。
type Stats struct {
	Received  uint64
	Accepted  uint64
	Filtered  uint64
	Rejected  uint64
	Delivered uint64
	Retried   uint64
}

// IDDeduper 是可报告已知事件 ID 的目标，用于重启后去重。
type IDDeduper interface {
	KnownIDs(ctx context.Context) ([]string, error)
}

// Pipeline 连接来源、过滤器、目标与位点存储。
type Pipeline struct {
	Sources    []source.Source
	Filter     *filter.Filter
	Sink       sink.Sink
	Checkpoint *checkpoint.Store
	QueueSize  int // 有界队列，目标变慢时提供背压
	// MaxRejections 限制内存中保留的拒绝记录条数，0 用默认值。
	MaxRejections int
	// RetryInterval 是目标不可用时的重试间隔，0 用默认值。
	RetryInterval time.Duration

	stats struct {
		received, accepted, filtered, rejected, delivered, retried atomic.Uint64
	}
	rejMu      sync.Mutex
	rejections []*event.Rejection
}

const defaultMaxRejections = 1024

// Run 启动并发采集与投递循环，直到 ctx 取消。
// 启动时校验位点，倒退直接报错；运行期目标不可用会重试并施加背压。
func (p *Pipeline) Run(ctx context.Context) error {
	if p.Sink == nil {
		return errors.New("pipeline: sink is required")
	}
	queueSize := p.QueueSize
	if queueSize <= 0 {
		queueSize = 256
	}

	// 1. 启动恢复：目标位点与本地位点比对，倒退直接报错。
	sinkSeq, err := p.Sink.LastSeq(ctx)
	if err != nil {
		return fmt.Errorf("read sink seq: %w", err)
	}
	if p.Checkpoint != nil {
		if err := p.Checkpoint.Verify(sinkSeq); err != nil {
			return err
		}
	}

	// 2. 去重集合：用目标已知 ID 预热，重启重放时不会重复落库。
	seen := map[string]struct{}{}
	if dd, ok := p.Sink.(IDDeduper); ok {
		ids, err := dd.KnownIDs(ctx)
		if err != nil {
			return fmt.Errorf("load known ids: %w", err)
		}
		for _, id := range ids {
			seen[id] = struct{}{}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	in := make(chan *event.Event, queueSize)    // 来源 → 调度
	queue := make(chan *event.Event, queueSize) // 调度 → 投递
	rej := make(chan *event.Rejection, queueSize)

	// 3. 并发采集。
	var srcWG sync.WaitGroup
	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}
	for _, s := range p.Sources {
		srcWG.Add(1)
		go func(s source.Source) {
			defer srcWG.Done()
			if err := s.Read(ctx, in, rej); err != nil && !errors.Is(err, context.Canceled) {
				reportErr(fmt.Errorf("source %s: %w", s.Name(), err))
			}
		}(s)
	}

	// 4. 调度：过滤、补全、去重、分配序号（单 goroutine 保证不重不漏）。
	var dispWG sync.WaitGroup
	dispWG.Add(1)
	go func() {
		defer dispWG.Done()
		nextSeq := sinkSeq + 1
		for {
			select {
			case <-ctx.Done():
				return
			case r := <-rej:
				p.addRejection(r)
			case e := <-in:
				p.stats.received.Add(1)
				if p.Filter != nil {
					if !p.Filter.Decide(e) {
						p.stats.filtered.Add(1)
						continue
					}
					p.Filter.Enrich(e)
				}
				if _, dup := seen[e.ID]; dup {
					p.stats.filtered.Add(1) // 重放/重复事件，确认过即跳过
					continue
				}
				seen[e.ID] = struct{}{}
				e.Seq = nextSeq
				nextSeq++
				p.stats.accepted.Add(1)
				select {
				case queue <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// 5. 投递：至少一次，成功后推进位点；目标不可用重试并靠有界队列背压。
	retry := p.RetryInterval
	if retry <= 0 {
		retry = 100 * time.Millisecond
	}
	deliver := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e := <-queue:
				for {
					if err := p.Sink.Write(ctx, e); err != nil {
						if errors.Is(err, context.Canceled) || ctx.Err() != nil {
							return ctx.Err()
						}
						p.stats.retried.Add(1)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(retry):
						}
						continue
					}
					break
				}
				if p.Checkpoint != nil {
					if err := p.Checkpoint.Save(e.Seq); err != nil {
						return err // 位点倒退等致命错误，直接报错
					}
				}
				p.stats.delivered.Add(1)
			}
		}
	}

	deliverErr := make(chan error, 1)
	go func() {
		if err := deliver(); err != nil && !errors.Is(err, context.Canceled) {
			deliverErr <- err
		}
		deliverErr <- nil
	}()

	// 6. 等待结束：外部取消、来源故障或投递致命错误。
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-errCh:
		runErr = err
	case err := <-deliverErr:
		if err != nil {
			runErr = err
		}
	}
	cancel()
	srcWG.Wait()
	dispWG.Wait()
	if runErr == nil {
		select {
		case err := <-deliverErr:
			runErr = err
		default:
		}
	}
	return runErr
}

// Stats 返回当前计数快照。
func (p *Pipeline) Stats() Stats {
	return Stats{
		Received:  p.stats.received.Load(),
		Accepted:  p.stats.accepted.Load(),
		Filtered:  p.stats.filtered.Load(),
		Rejected:  p.stats.rejected.Load(),
		Delivered: p.stats.delivered.Load(),
		Retried:   p.stats.retried.Load(),
	}
}

// Rejections 返回运行期收集到的拒绝记录（有界）。
func (p *Pipeline) Rejections() []*event.Rejection {
	p.rejMu.Lock()
	defer p.rejMu.Unlock()
	out := make([]*event.Rejection, len(p.rejections))
	copy(out, p.rejections)
	return out
}

func (p *Pipeline) addRejection(r *event.Rejection) {
	if r == nil {
		return
	}
	p.stats.rejected.Add(1)
	max := p.MaxRejections
	if max <= 0 {
		max = defaultMaxRejections
	}
	p.rejMu.Lock()
	defer p.rejMu.Unlock()
	if len(p.rejections) < max {
		p.rejections = append(p.rejections, r)
	}
}
