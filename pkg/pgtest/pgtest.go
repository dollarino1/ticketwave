// Package pgtest gives integration tests a private, freshly migrated Postgres
// schema, so they never read or modify real development data.
package pgtest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool connects to the database whose URL is in the environment variable
// envVar and returns a pool whose search_path is a brand-new schema holding
// every *.up.sql migration found under migrationsDir, a path relative to the
// repository root. The schema is dropped when the test ends.
//
// The test is skipped when the variable is unset, so a plain `go test` needs no
// database. Running against the compose Postgres looks like:
//
//	ORDER_DATABASE_URL="postgres://ticketwave:ticketwave@localhost:5433/orders?sslmode=disable" \
//	  go test -tags integration ./services/order/...
func NewPool(t testing.TB, envVar, migrationsDir string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv(envVar)
	if url == "" {
		t.Skipf("%s not set", envVar)
	}
	ctx := context.Background()

	admin, err := pgxpool.New(ctx, url)
	check(t, err)
	schema := "pgtest_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	_, err = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	check(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})

	cfg, err := pgxpool.ParseConfig(url)
	check(t, err)
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	check(t, err)
	t.Cleanup(pool.Close)

	files, err := filepath.Glob(filepath.Join(repoRoot(t), migrationsDir, "*.up.sql"))
	check(t, err)
	if len(files) == 0 {
		t.Fatalf("found no migrations under %s", migrationsDir)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		check(t, err)
		_, err = pool.Exec(ctx, string(sql))
		check(t, err)
	}
	return pool
}

// WarmPool opens every connection the pool will ever use. A pool opens them
// lazily, so without this the "simultaneous" callers of a concurrency test would
// arrive staggered by connection-setup time and mostly run one after another,
// which hides exactly the races such a test exists to expose.
func WarmPool(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	n := int(pool.Config().MaxConns)
	conns := make([]*pgxpool.Conn, 0, n)
	for i := 0; i < n; i++ {
		c, err := pool.Acquire(context.Background())
		check(t, err)
		conns = append(conns, c)
	}
	for _, c := range conns {
		c.Release()
	}
}

func check(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// repoRoot walks up from the test's working directory to the folder holding go.mod.
func repoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	check(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found in any parent directory")
		}
		dir = parent
	}
}
