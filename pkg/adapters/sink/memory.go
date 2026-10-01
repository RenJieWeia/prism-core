package sink

import (
	"context"
	"sync"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// MemorySink 内存输出端口
// 保存收到的处理结果，主要用于测试。
type MemorySink struct {
	id      string
	mu      sync.Mutex
	results []domain.ProcessingResult
}

// NewMemorySink 创建内存输出端口
func NewMemorySink() *MemorySink {
	return &MemorySink{id: "memory"}
}

// ID 实现 ports.ResultSink 接口
func (s *MemorySink) ID() string {
	return s.id
}

// Deliver 实现 ports.ResultSink 接口
func (s *MemorySink) Deliver(_ context.Context, result domain.ProcessingResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, result)
	return nil
}

// Results 返回收到的处理结果 (拷贝)
func (s *MemorySink) Results() []domain.ProcessingResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.ProcessingResult, len(s.results))
	copy(out, s.results)
	return out
}

// Count 返回收到的处理结果数量
func (s *MemorySink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.results)
}
