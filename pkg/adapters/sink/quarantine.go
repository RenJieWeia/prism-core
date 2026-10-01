package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// QuarantineSink 隔离数据输出端口
// 将处理结果中的 Rejected 异常数据逐条保存到 QuarantineRepository。
// 使用调用方传入的 ctx，不创建独立 Background Context，不启动 Goroutine。
type QuarantineSink struct {
	id   string
	repo ports.QuarantineRepository
}

// NewQuarantineSink 创建隔离数据输出端口
func NewQuarantineSink(repo ports.QuarantineRepository) *QuarantineSink {
	return &QuarantineSink{id: "quarantine", repo: repo}
}

// ID 实现 ports.ResultSink 接口
func (s *QuarantineSink) ID() string {
	return s.id
}

// Deliver 实现 ports.ResultSink 接口
// 只处理 ProcessingResult.Rejected，逐条调用 QuarantineRepository.Save。
// 保存失败时返回明确错误 (含 Sink ID / 设备 ID / 数据时间 / 原始错误)。
func (s *QuarantineSink) Deliver(ctx context.Context, result domain.ProcessingResult) error {
	if len(result.Rejected) == 0 {
		return nil
	}

	var errs []error
	for _, q := range result.Rejected {
		if err := s.repo.Save(ctx, q); err != nil {
			errs = append(errs, fmt.Errorf(
				"sink %q save quarantine failed: device %s timestamp %s: %w",
				s.id, q.Reading.DeviceInfo.ID, q.Reading.Timestamp.Format(time.RFC3339), err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
