package sim

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The simulator is only as deterministic as the cores it runs. This test
// enforces the sans-I/O rule mechanically: core packages may not start
// goroutines, read the clock, use global randomness or import I/O.
var corePackages = []string{"agent", "sequencer", "cache", "placement", "proc", "metalog", "segment", "kbatch", "checker"}

var forbiddenImports = map[string]bool{
	"net": true, "net/http": true, "os": true, "os/exec": true, "math/rand": true, "math/rand/v2": true,
	"crypto/rand": true, "sync": true, "sync/atomic": true, "io/ioutil": true, "log": true, "log/slog": true,
	"google.golang.org/grpc": true,
}

var forbiddenTime = map[string]bool{
	"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "AfterFunc": true,
	"Tick": true, "NewTimer": true, "NewTicker": true,
}

func TestCoresArePure(t *testing.T) {
	for _, pkg := range corePackages {
		dir := filepath.Join("..", pkg)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			path := filepath.Join(dir, name)
			f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			timeAlias := ""
			for _, imp := range f.Imports {
				p, _ := strconv.Unquote(imp.Path.Value)
				// The checker reads buckets through objstore, which is fine
				// offline; it must still not touch clocks or goroutines.
				if forbiddenImports[p] && (pkg != "checker" || p != "os") {
					t.Errorf("%s imports %s", path, p)
				}
				if p == "time" {
					timeAlias = "time"
					if imp.Name != nil {
						timeAlias = imp.Name.Name
					}
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.GoStmt:
					t.Errorf("%s starts a goroutine", path)
				case *ast.SelectorExpr:
					if id, ok := n.X.(*ast.Ident); ok && timeAlias != "" && id.Name == timeAlias && forbiddenTime[n.Sel.Name] {
						t.Errorf("%s calls time.%s; take time from Step's now", path, n.Sel.Name)
					}
				}
				return true
			})
		}
	}
}
