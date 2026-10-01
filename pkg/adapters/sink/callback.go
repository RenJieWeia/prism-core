package sink

import (
	"context"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// CallbackSink 回调输出端口
// 允许调用方传入函数处理处理结果。
type CallbackSink struct {
	id string
	fn func(ctx context.Context, result domain.ProcessingResult) error
}

// NewCallbackSink 创建回调输出端口
func NewCallbackSink(id string, fn func(ctx context.Context, result domain.ProcessingResult) error) *CallbackSink {
	return &CallbackSink{id: id, fn: fn}
}

// ID 实现 ports.ResultSink 接口
func (s *CallbackSink) ID() string {
	return s.id
}

// Deliver 实现 ports.ResultSink 接口
func (s *CallbackSink) Deliver(ctx context.Context, result domain.ProcessingResult) error {
	return s.fn(ctx, result)
}
