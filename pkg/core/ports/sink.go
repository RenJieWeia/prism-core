package ports

import (
	"context"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// ResultSink 处理结果输出端口
// 将完整的处理结果交付给下游，由适配器层实现具体的投递方式。
// Core 不负责投递，由 Application Pipeline (pkg/application/pipeline) 调用。
type ResultSink interface {
	// ID 输出端唯一标识
	ID() string

	// Deliver 交付一次处理结果
	Deliver(
		ctx context.Context,
		result domain.ProcessingResult,
	) error
}
