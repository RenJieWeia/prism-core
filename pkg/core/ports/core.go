package ports

import (
	"context"
	"io"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// UniversalIngestor 万能插头 (Ingestion Layer)
// 业务背景: 别的平台最怕对接... 我们设计了一个万能插头。
// 职责: 接收任意格式、任意频率的数据，统一接入。
type UniversalIngestor interface {
	// IngestStream 接入实时流数据 (JSON, Binary, etc.)
	IngestStream(ctx context.Context, stream io.Reader) (*domain.IngestionResult, error)

	// IngestBatch 接入批量文件 (Excel, CSV)
	IngestBatch(ctx context.Context, file io.Reader, format string) (*domain.IngestionResult, error)
}

// EnergyDataStandardizer 能源数据标准化服务 (Core Capability)
// 核心竞争力: 帮下游平台“避坑” & 输出“数据标准”
// 职责:
// A. 数据清洗 (Duplicate/Null/Jump removal)
// B. 精度对齐 (Float -> Scaled Int)
// C. 频率对齐 (Minute -> Hour/Standard Snapshot)
type EnergyDataStandardizer interface {
	// GetStandardReading 获取特定时间点的“标准读数”
	// 描述: “某设备在某时间点的标准读数是多少？” -> 清洗过、精度对齐的标准答案。
	GetStandardReading(ctx context.Context, deviceID string, timestamp time.Time) (*domain.StandardReading, error)

	// ProcessAndStandardize 直接处理输入数据并返回标准集 (用于即时转换场景)
	ProcessAndStandardize(ctx context.Context, rawReadings []domain.Reading) ([]domain.StandardReading, error)
}

// EnergyDataProcessor 支持完整处理流程的标准化服务接口 (EnergyDataStandardizer 的扩展)
type EnergyDataProcessor interface {
	EnergyDataStandardizer

	// Process 完整处理输入数据并返回完整处理结果 (含接受/拒绝/规则评估)
	// ProcessAndStandardize 是其简化版，只返回 Accepted。
	Process(ctx context.Context, rawReadings []domain.Reading) (domain.ProcessingResult, error)
}

// Aligner 定义时间对齐能力的接口
// 核心职责：从散乱的时间序列中提取特定时间点的快照
type Aligner interface {
	// FindSnapshot 在已排序的readings中查找最接近target时间点的读数
	// 注意: readings 必须按 Timestamp 升序排列
	FindSnapshot(readings []domain.Reading, target time.Time) *domain.Reading
}
