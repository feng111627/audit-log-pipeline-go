// Package pipeline 串联采集、过滤、补全、串联与投递，提供背压与拒绝统计。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/feng111627/audit-log-pipeline-go/internal/chain"
	"github.com/feng111627/audit-log-pipeline-go/internal/event"
	"github.com/feng111627/audit-log-pipeline-go/internal/filter"
	"github.com/feng111627/audit-log-pipeline-go/internal/sink"
	"github.com/feng111627/audit-log-pipeline-go/internal/source"
)

// Config 控制管道行为。
type Config struct {
	QueueSize     int               // 有界队列容量，满了即背压
	BatchSize     int               // 每批投递的记录数
	EnrichWith    map[string]string // 字段补全默认值
	RetryBackoff  time.Duration     // 目标不可用时的重试间隔，默认 100ms
	FlushInterval time.Duration     // 部分批次定时冲刷间隔，默认 200ms
	Logger        *log.Logger       // 可选日志
}

// Pipeline 是采集-转发管道。
type Pipeline struct {
	cfg     Config
	sources []source.Source
	filter  *filter.Filter
	sink    sink.Sink

	mu         sync.Mutex
	rejections []event.Rejection
	ingested   int64
	delivered  int64
	duplicates int64
}

// New 创建管道。
func New(cfg Config, sources []source.Source, f *filter.Filter, sk sink.Sink) *Pipeline {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 64
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 100 * time.Millisecond
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	return &Pipeline{cfg: cfg, sources: sources, filter: f, sink: sk}
}

// Run 启动所有来源并发采集并投递，直到 ctx 取消；返回前冲刷剩余批次。
func (p *Pipeline) Run(ctx context.Context) error {
	rawCh := make(chan []byte, p.cfg.QueueSize) // 有界队列：满则来源阻塞，形成背压

	var wg sync.WaitGroup
	errCh := make(chan error, len(p.sources))
	for _, src := range p.sources {
		wg.Add(1)
		go func(s source.Source) {
			defer wg.Done()
			if err := s.Run(ctx, rawCh); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("source %s: %w", s.Name(), err)
			}
		}(src)
	}
	go func() {
		wg.Wait()
		close(rawCh)
	}()

	derr := p.dispatch(ctx, rawCh)

	select {
	case err := <-errCh:
		return err
	default:
	}
	return derr
}

// dispatch 是唯一的串行处理循环：解析→校验→补全→过滤→串联→批量投递。
// 单点串联保证哈希链一致；单点计数保证并发采集下不重复、不跟丢。
func (p *Pipeline) dispatch(ctx context.Context, rawCh <-chan []byte) error {
	chainer := p.resumeChainer()
	seen := make(map[string]struct{})
	var batch []chain.Record
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		for {
			err := p.sink.WriteBatch(ctx, batch)
			if err == nil {
				p.addDelivered(int64(len(batch)))
				batch = batch[:0]
				return nil
			}
			if errors.Is(err, sink.ErrOffsetRegression) {
				// 位点倒退是不可恢复错误，直接报错，绝不从中间接着写。
				return err
			}
			p.logf("sink %s unavailable: %v; retrying in %s", p.sink.Name(), err, p.cfg.RetryBackoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.cfg.RetryBackoff):
			}
		}
	}

	for {
		select {
		case raw, ok := <-rawCh:
			if !ok {
				return flush()
			}
			e, rej := event.Parse(raw)
			if rej != nil {
				p.addRejection(*rej)
				continue
			}
			e = event.Enrich(e, p.cfg.EnrichWith)
			if rej := event.Validate(e); rej != nil {
				p.addRejection(*rej)
				continue
			}
			if _, dup := seen[e.ID]; dup {
				p.mu.Lock()
				p.duplicates++
				p.mu.Unlock()
				continue
			}
			seen[e.ID] = struct{}{}
			if rule, hit := p.filter.Match(e); hit {
				p.addRejection(event.Rejection{
					Reason: event.RejectFiltered,
					Detail: fmt.Sprintf("matched rule %+v", rule),
					Raw:    string(raw),
				})
				continue
			}
			batch = append(batch, chainer.Append(e))
			p.mu.Lock()
			p.ingested++
			p.mu.Unlock()
			if len(batch) >= p.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			// 定时冲刷部分批次，避免尾部事件滞留。
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			// 退出前尽力冲刷（脱离已取消的 ctx），保证不丢已采集事件。
			if err := p.sink.WriteBatch(context.WithoutCancel(ctx), batch); err == nil {
				p.addDelivered(int64(len(batch)))
				batch = batch[:0]
			} else if !errors.Is(err, context.Canceled) {
				return err
			}
			return ctx.Err()
		}
	}
}

// resumeChainer 从支持状态查询的 sink 恢复链尾，实现重启后续链。
func (p *Pipeline) resumeChainer() *chain.Chainer {
	type stater interface {
		State() (int64, string)
	}
	if st, ok := p.sink.(stater); ok {
		committed, lastHash := st.State()
		return chain.NewChainer(committed+1, lastHash)
	}
	return chain.NewChainer(1, chain.GenesisHash)
}

func (p *Pipeline) addRejection(r event.Rejection) {
	p.mu.Lock()
	p.rejections = append(p.rejections, r)
	p.mu.Unlock()
	p.logf("rejected [%s] %s", r.Reason, r.Detail)
}

func (p *Pipeline) addDelivered(n int64) {
	p.mu.Lock()
	p.delivered += n
	p.mu.Unlock()
}

func (p *Pipeline) logf(format string, args ...any) {
	if p.cfg.Logger != nil {
		p.cfg.Logger.Printf(format, args...)
	}
}

// Rejections 返回所有被拒绝的事件记录（含原因）。
func (p *Pipeline) Rejections() []event.Rejection {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]event.Rejection, len(p.rejections))
	copy(out, p.rejections)
	return out
}

// Stats 返回采集/投递/拒绝/重复计数，用于核对不重复不跟丢。
func (p *Pipeline) Stats() (ingested, delivered, rejected, duplicates int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ingested, p.delivered, int64(len(p.rejections)), p.duplicates
}
