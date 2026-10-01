package ports

import (
	"context"
	"time"

	"github.com/RenJieWeia/prism-core/pkg/core/domain"
)

// UpsertStrategy 定义数据持久化时的冲突解决策略
type UpsertStrategy string

const (
	// UpsertStrategyLastWriteWins 盲目覆盖 (Last Writer Wins)
	// 无论新旧数据优先级如何，总是用新数据覆盖旧数据。
	UpsertStrategyLastWriteWins UpsertStrategy = "LAST_WRITE_WINS"

	// UpsertStrategyHighPriorityWins 优先级竞争 (Best Quality Wins)
	// 只有当新数据的 Priority >= 旧数据的 Priority 时才更新。
	// 适用于补数据、人工校正等场景。
	UpsertStrategyHighPriorityWins UpsertStrategy = "HIGH_PRIORITY_WINS"
)

// StandardReadingRepository 标准读数仓储接口
// 对应核心竞争力: 输出“数据标准”的持久化载体
// 职责: 存储经过 Standardizer 清洗和对齐后的“黄金数据”，供下游查询整个园区/工厂的标准历史。
type StandardReadingRepository interface {
	// Save 保存单个标准读数 (需指定冲突策略)
	Save(ctx context.Context, reading domain.StandardReading, strategy UpsertStrategy) error

	// SaveBatch 批量保存 (需指定冲突策略)
	SaveBatch(ctx context.Context, readings []domain.StandardReading, strategy UpsertStrategy) error

	// FindExact 获取特定时间点的标准读数 (对应 GetStandardReading)
	// 场景: "获取 D1 设备在 10:00:00 的确切标准读数"
	FindExact(ctx context.Context, deviceID string, timestamp time.Time) (*domain.StandardReading, error)

	// FindRange 获取时间范围内的标准读数
	// 场景: 报表生成、趋势分析
	FindRange(ctx context.Context, deviceID string, start, end time.Time) ([]domain.StandardReading, error)
}

// CleaningRuleRepository 清洗规则仓储接口
// 职责: 管理数据清洗的规则配置，Standardizer 启动或运行时通过此接口加载规则
type CleaningRuleRepository interface {
	// Save 保存或更新规则
	Save(ctx context.Context, rule domain.CleaningRule) error

	// GetByID 获取指定规则
	GetByID(ctx context.Context, id string) (*domain.CleaningRule, error)

	// ListByDeviceType 获取适用于特定设备类型的所有规则
	ListByDeviceType(ctx context.Context, deviceType domain.DeviceType) ([]domain.CleaningRule, error)

	// ListEnabledByDeviceType 获取特定设备类型下所有启用的规则
	ListEnabledByDeviceType(ctx context.Context, deviceType domain.DeviceType) ([]domain.CleaningRule, error)

	// Delete 删除规则
	Delete(ctx context.Context, id string) error
}

// DeviceRepository 设备元数据仓储 (可选，视校验需求而定)
type DeviceRepository interface {
	// Exists 检查设备是否存在
	Exists(ctx context.Context, deviceID string) (bool, error)
}

// QuarantineRepository 隔离区仓储接口
// 职责: 存储被“拒收”或需“人工审核”的脏数据，供后续治理
type QuarantineRepository interface {
	// Save 保存一条隔离记录 (新增或更新状态)
	Save(ctx context.Context, record domain.QuarantineReading) error

	// FindPending 获取待处理的隔离记录
	// 场景: 数据管理员拉取 "NEW" 或 "REVIEW_NEEDED" 的数据进行处理
	FindPending(ctx context.Context, limit int) ([]domain.QuarantineReading, error)
}
