package sink

import (
	"context"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// RepositorySink 仓储输出端口
// 将处理结果中的 Accepted 标准数据写入现有 StandardReadingRepository。
type RepositorySink struct {
	id       string
	repo     ports.StandardReadingRepository
	strategy ports.UpsertStrategy
}

// NewRepositorySink 创建仓储输出端口
func NewRepositorySink(repo ports.StandardReadingRepository, strategy ports.UpsertStrategy) *RepositorySink {
	return &RepositorySink{id: "repository", repo: repo, strategy: strategy}
}

// ID 实现 ports.ResultSink 接口
func (s *RepositorySink) ID() string {
	return s.id
}

// Deliver 实现 ports.ResultSink 接口
// 仅保存 Accepted 中的标准数据，隔离数据由隔离区仓储处理。
func (s *RepositorySink) Deliver(ctx context.Context, result domain.ProcessingResult) error {
	if len(result.Accepted) == 0 {
		return nil
	}
	return s.repo.SaveBatch(ctx, result.Accepted, s.strategy)
}
