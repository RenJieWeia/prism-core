package boundaries_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// repoRoot 返回仓库根目录 (通过本测试文件路径推导, 不依赖工作目录)
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	// tests/core/boundaries/boundaries_test.go -> 仓库根
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(file))))
}

// TestCoreImportBoundaries 架构边界: pkg/core 不得导入 application / adapters 及
// 数据库、HTTP、日志等外部设施。
func TestCoreImportBoundaries(t *testing.T) {
	forbidden := []string{
		"github.com/RenJieWeia/prism-core/pkg/application",
		"github.com/RenJieWeia/prism-core/pkg/adapters",
		"database/sql",
		"net/http",
		"log",
		"log/slog",
	}

	coreDir := filepath.Join(repoRoot(t), "pkg", "core")
	fset := token.NewFileSet()

	err := filepath.WalkDir(coreDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if perr != nil {
			t.Errorf("parse %s: %v", path, perr)
			return nil
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			for _, bad := range forbidden {
				if p == bad || strings.HasPrefix(p, bad+"/") {
					t.Errorf("pkg/core boundary violation: %s imports forbidden package %q", path, p)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk pkg/core: %v", err)
	}
}
