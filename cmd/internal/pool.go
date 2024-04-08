package internal

import (
	"context"
	"fmt"

	"github.com/ingcr3at1on/pjs/internal/env"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == `` {
		dsn = env.ParseAlternateSettings()
	}

	pc, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.ParseConfig -- %w", err)
	}
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	pc.ConnConfig.DescriptionCacheCapacity = 1024

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.ConnectConfig -- %w", err)
	}

	return pool, nil
}
