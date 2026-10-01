// Package pipeline 提供应用层处理管线，负责 Core 之外的交付职责:
// 保存标准数据 / 保存隔离数据 / 投递下游 Sink / 错误聚合。
package pipeline

import (
	"context"
	"errors"
	"fmt"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
	"github.com/RenJieWeia/prism-core/pkg/core/ports"
)

// Pipeline 处理管线接口
type Pipeline interface {
	// Execute 执行完整处理流程并返回处理结果与交付错误。
	// 即使交付 (保存/Sink 投递) 失败，也会返回已经生成的 ProcessingResult。
	Execute(
		ctx context.Context,
		rawReadings []domain.Reading,
	) (domain.ProcessingResult, error)
}

// ProcessingPipeline 应用层处理管线实现
// 职责:
//   - 调用 Core Processor (ports.EnergyDataProcessor) 生成 ProcessingResult
//   - 通过 standardSink 保存 Accepted (如 RepositorySink)
//   - 通过 quarantineSink 保存 Rejected (如 QuarantineSink)
//   - 将完整结果投递给其他 resultSinks
//
// 依赖方向: adapters → application → core/ports → core/domain。
type ProcessingPipeline struct {
	processor      ports.EnergyDataProcessor // Core 处理器
	standardSink   ports.ResultSink          // 保存 Accepted (可为 nil)
	quarantineSink ports.ResultSink          // 保存 Rejected (可为 nil)
	resultSinks    []ports.ResultSink        // 其他: 接收完整结果
}

// NewProcessingPipeline 创建处理管线
// standardSink / quarantineSink 可为 nil (跳过对应保存)。
func NewProcessingPipeline(
	processor ports.EnergyDataProcessor,
	standardSink ports.ResultSink,
	quarantineSink ports.ResultSink,
	resultSinks ...ports.ResultSink,
) *ProcessingPipeline {
	return &ProcessingPipeline{
		processor:      processor,
		standardSink:   standardSink,
		quarantineSink: quarantineSink,
		resultSinks:    resultSinks,
	}
}

// Execute 实现 Pipeline 接口
// 执行顺序:
//  1. Core Process
//  2. 保存 Accepted (standardSink)
//  3. 保存 Rejected (quarantineSink)
//  4. 投递其他 resultSinks
//  5. 返回 (result, errors.Join(交付错误))
//
// 若 Core Process 失败 (清洗/参考解析/配置/标准化/Context 错误)，直接返回 (result, coreErr)，
// 不执行交付，避免把部分/失败结果写入下游。
func (p *ProcessingPipeline) Execute(ctx context.Context, rawReadings []domain.Reading) (domain.ProcessingResult, error) {
	result, err := p.processor.Process(ctx, rawReadings)
	if err != nil {
		return result, err
	}

	var deliveryErrs []error
	deliveryErrs = append(deliveryErrs, p.deliver(p.standardSink, ctx, result)...)
	deliveryErrs = append(deliveryErrs, p.deliver(p.quarantineSink, ctx, result)...)
	for _, sink := range p.resultSinks {
		deliveryErrs = append(deliveryErrs, p.deliver(sink, ctx, result)...)
	}

	if len(deliveryErrs) > 0 {
		return result, errors.Join(deliveryErrs...)
	}
	return result, nil
}

// deliver 投递单个 Sink，错误包含 Sink ID 与原始错误
func (p *ProcessingPipeline) deliver(s ports.ResultSink, ctx context.Context, result domain.ProcessingResult) []error {
	if s == nil {
		return nil
	}
	if err := s.Deliver(ctx, result); err != nil {
		return []error{fmt.Errorf("sink %q deliver failed: %w", s.ID(), err)}
	}
	return nil
}
