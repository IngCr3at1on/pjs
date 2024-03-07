package pjs

import (
	"context"
	"fmt"
	"strings"

	"github.com/ingcr3at1on/pjs/pkg/scanner"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Receiver is a value receiver when reading postgres data.
type Receiver struct {
	Name        string
	DataTypeOID uint32
	Val         interface{}
}

// Transpose reads arbitrary data out of pgx.Rows.
func Transpose(
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	transposeF func(receivers []Receiver) error,
) error {
	sql = strings.TrimSpace(sql)

	batch, err := scanner.ScanQueries(sql)
	if err != nil {
		return fmt.Errorf("postgres.ScanQueries: %w", err)
	}

	res := pool.SendBatch(ctx, &batch)
	defer res.Close()

	for i := 0; i < batch.Len(); i++ {
		if err = processOneQueryResponse(res, transposeF); err != nil {
			return fmt.Errorf("processOneQueryResponse[%d] -- %w", i, err)
		}
	}

	return nil
}

func processOneQueryResponse(res pgx.BatchResults, transposeF func(receivers []Receiver) error) error {
	rows, err := res.Query()
	if err != nil {
		return fmt.Errorf("pool.Query -- %w", err)
	}
	defer rows.Close()

	descs := rows.FieldDescriptions()

	for rows.Next() {
		recs, vals := getReceivers(descs)
		if err = rows.Scan(vals...); err != nil {
			return fmt.Errorf("rows.Scan -- %w", err)
		}

		if err = transposeF(recs); err != nil {
			return fmt.Errorf("transposeF -- %w", err)
		}
	}

	return rows.Err()
}

func getReceivers(descs []pgconn.FieldDescription) ([]Receiver, []interface{}) {
	l := len(descs)
	recs := make([]Receiver, l)
	vals := make([]interface{}, l)
	for n, desc := range descs {
		r := Receiver{
			Name:        desc.Name,
			DataTypeOID: desc.DataTypeOID,
			Val:         getOb(desc.DataTypeOID),
		}
		recs[n] = r
		vals[n] = r.Val
	}
	return recs, vals
}
