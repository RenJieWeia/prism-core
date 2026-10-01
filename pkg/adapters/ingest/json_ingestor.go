package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// JsonUniversalIngestor 实现 UniversalIngestor 接口
// 专门处理 JSON 格式的数据流
type JsonUniversalIngestor struct {
	// downstream 是数据流向的下一站
	// 在实际系统中，这里可能是调用 Standardizer.ProcessAndStandardize
	// 或者推送到消息队列
	downstream func(context.Context, []domain.Reading) error
}

func NewJsonUniversalIngestor(downstream func(context.Context, []domain.Reading) error) *JsonUniversalIngestor {
	return &JsonUniversalIngestor{
		downstream: downstream,
	}
}

// IngestStream 实现 UniversalIngestor.IngestStream
// 简化版：我们假设输入总是 JSON 数组 [...]，以规避 decoder.Token 的复杂性
func (j *JsonUniversalIngestor) IngestStream(ctx context.Context, stream io.Reader) (*domain.IngestionResult, error) {
	// 使用 bufio.Reader 预读首字节，避免消耗 Token
	bufStream := bufio.NewReader(stream)
	head, err := bufStream.Peek(1)
	if err != nil {
		if err == io.EOF {
			return &domain.IngestionResult{}, nil
		}
		return nil, fmt.Errorf("failed to peek start token: %w", err)
	}

	decoder := json.NewDecoder(bufStream)
	result := &domain.IngestionResult{}

	// Case 1: JSON Array [...]
	if head[0] == '[' {
		// Consume '['
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return j.decodeArray(ctx, decoder, result)
	}

	// Case 2: Single JSON Object {...}
	if head[0] == '{' {
		var p rawPayload
		if err := decoder.Decode(&p); err != nil {
			return nil, fmt.Errorf("failed to decode single object: %w", err)
		}

		result.Total++
		reading, err := j.mapToDomain(p)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("mapping error: %v", err))
			return result, nil // Partial success? Or we consider single object fail as total fail.
		}

		if err := j.downstream(ctx, []domain.Reading{reading}); err != nil {
			return nil, err
		}
		result.Success++
		return result, nil
	}

	return nil, fmt.Errorf("unexpected JSON format (expected '[' or '{', got '%c')", head[0])
}

// IngestBatch 实现 UniversalIngestor.IngestBatch
func (j *JsonUniversalIngestor) IngestBatch(ctx context.Context, file io.Reader, format string) (*domain.IngestionResult, error) {
	if format != "json" {
		return nil, fmt.Errorf("unsupported format for JsonIngestor: %s", format)
	}
	return j.IngestStream(ctx, file)
}

// --- Internal Parsing Logic ---

// rawPayload 定义接收的扁平化 JSON 结构
// 适配多种字段命名风格 (Snake Case / Camel Case)
type rawPayload struct {
	DeviceID  string      `json:"device_id"`
	Model     string      `json:"model"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"` // 支持 RFC3339 或 简单时间格式
	Value     json.Number `json:"value"`     // 使用 json.Number 避免精度丢失
}

func (j *JsonUniversalIngestor) decodeArray(ctx context.Context, decoder *json.Decoder, result *domain.IngestionResult) (*domain.IngestionResult, error) {
	var buffer []domain.Reading
	const batchSize = 100 // 简单的批处理缓冲

	// while decoder.More()
	for decoder.More() {
		var p rawPayload
		if err := decoder.Decode(&p); err != nil {
			return nil, fmt.Errorf("decode error inside array: %w", err)
		}

		result.Total++
		r, err := j.mapToDomain(p)
		if err != nil {
			// 策略：记录错误并继续
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("item %d skipped: %v", result.Total, err))
			continue
		}

		buffer = append(buffer, r)
		result.Success++

		// Flush buffer if full
		if len(buffer) >= batchSize {
			if err := j.downstream(ctx, buffer); err != nil {
				return result, err
			}
			buffer = buffer[:0] // clear
		}
	}

	// Flush remaining
	if len(buffer) > 0 {
		if err := j.downstream(ctx, buffer); err != nil {
			return result, err
		}
	}

	// Consume closing ']'
	if _, err := decoder.Token(); err != nil {
		return result, err
	}
	return result, nil
}

// mapToDomain 将扁平 JSON 转换为领域对象
func (j *JsonUniversalIngestor) mapToDomain(p rawPayload) (domain.Reading, error) {
	// 1. Time Parsing
	var ts time.Time
	var err error

	// 尝试标准格式
	if ts, err = time.Parse(time.RFC3339, p.Timestamp); err != nil {
		// 尝试常见格式
		if ts, err = time.Parse("2006-01-02 15:04:05", p.Timestamp); err != nil {
			return domain.Reading{}, fmt.Errorf("invalid timestamp format: %s", p.Timestamp)
		}
	}

	// 2. Value Parsing
	val, err := p.Value.Float64()
	if err != nil {
		return domain.Reading{}, fmt.Errorf("invalid value format: %v", p.Value)
	}

	return domain.Reading{
		DeviceInfo: domain.DeviceInfo{
			ID:    p.DeviceID,
			Model: p.Model,
			Type:  domain.DeviceType(p.Type),
		},
		Timestamp: ts,
		Value:     val,
	}, nil
}
