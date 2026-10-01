package ingest

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// CsvUniversalIngestor 实现 UniversalIngestor 接口
// 专门处理 CSV 格式的数据流
type CsvUniversalIngestor struct {
	downstream func(context.Context, []domain.Reading) error
}

// NewCsvUniversalIngestor 创建 CSV 摄入器实例
func NewCsvUniversalIngestor(downstream func(context.Context, []domain.Reading) error) *CsvUniversalIngestor {
	return &CsvUniversalIngestor{
		downstream: downstream,
	}
}

// IngestStream 实现 UniversalIngestor.IngestStream
// 逐行读取 CSV 流
func (c *CsvUniversalIngestor) IngestStream(ctx context.Context, stream io.Reader) (*domain.IngestionResult, error) {
	reader := csv.NewReader(stream)
	// 允许变长字段，避免因某些行缺少非必填字段报错
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	result := &domain.IngestionResult{}

	// 1. Read Header
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return result, nil
		}
		return nil, fmt.Errorf("failed to read csv header: %w", err)
	}

	headerMap := make(map[string]int)
	for i, h := range headers {
		headerMap[strings.ToLower(strings.TrimSpace(h))] = i
	}

	// Validate required columns
	if err := validateCsvHeaders(headerMap); err != nil {
		return nil, err
	}

	var buffer []domain.Reading
	const batchSize = 100

	// 2. Read Records
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			result.Total++
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("csv read error at line %d: %v", result.Total+1, err)) // +1 for header
			continue
		}

		result.Total++
		reading, err := c.parseRecord(record, headerMap)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: %v", result.Total+1, err))
			continue
		}

		buffer = append(buffer, reading)
		result.Success++

		if len(buffer) >= batchSize {
			if err := c.downstream(ctx, buffer); err != nil {
				return result, err
			}
			buffer = buffer[:0]
		}
	}

	if len(buffer) > 0 {
		if err := c.downstream(ctx, buffer); err != nil {
			return result, err
		}
	}

	return result, nil
}

// IngestBatch 实现 UniversalIngestor.IngestBatch
func (c *CsvUniversalIngestor) IngestBatch(ctx context.Context, file io.Reader, format string) (*domain.IngestionResult, error) {
	if strings.ToLower(format) != "csv" {
		return nil, fmt.Errorf("unsupported format for CsvIngestor: %s", format)
	}
	return c.IngestStream(ctx, file)
}

func validateCsvHeaders(headerMap map[string]int) error {
	required := []string{"device_id", "timestamp", "value"}
	for _, req := range required {
		if _, ok := headerMap[req]; !ok {
			return fmt.Errorf("missing required csv header: %s", req)
		}
	}
	return nil
}

func (c *CsvUniversalIngestor) parseRecord(record []string, headerMap map[string]int) (domain.Reading, error) {
	// Helper to get value gracefully
	get := func(col string) string {
		if idx, ok := headerMap[col]; ok && idx < len(record) {
			return record[idx]
		}
		return ""
	}

	// 1. Device Info
	deviceID := get("device_id")
	if deviceID == "" {
		return domain.Reading{}, fmt.Errorf("device_id is empty")
	}

	// 2. Timestamp
	tsStr := get("timestamp")
	var ts time.Time
	var err error
	// Try standard ISO8601/RFC3339 first
	if ts, err = time.Parse(time.RFC3339, tsStr); err != nil {
		// Try SQL like format
		if ts, err = time.Parse("2006-01-02 15:04:05", tsStr); err != nil {
			// Try other common formats if needed
			return domain.Reading{}, fmt.Errorf("invalid timestamp format: %s", tsStr)
		}
	}

	// 3. Value
	valStr := get("value")
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return domain.Reading{}, fmt.Errorf("invalid value format: %s", valStr)
	}

	return domain.Reading{
		DeviceInfo: domain.DeviceInfo{
			ID:    deviceID,
			Model: get("model"),
			Type:  domain.DeviceType(get("type")),
		},
		Timestamp: ts,
		Value:     val,
	}, nil
}
