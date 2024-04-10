package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ingcr3at1on/pjs"
	"github.com/ingcr3at1on/pjs/cmd/internal"
	"github.com/ingcr3at1on/pjs/internal/customq"
	"github.com/ingcr3at1on/x/sigctx"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	dsnFK   = `dsn`
	typesFK = `show-types`

	envQueriesDir = `PJS_CUSTOM_QUERIES`
)

var (
	dsnF   *string
	typesF *bool

	root = &cobra.Command{
		Use:           "pjs",
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       cobra.NoArgs,
		RunE:          transpose,
	}
)

func init() {
	flags := root.PersistentFlags()
	dsnF = flags.StringP(dsnFK, `d`, ``, `a dsn string`)
	typesF = flags.BoolP(typesFK, `t`, false, `show types`)

	if err := initImpl(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func initImpl() error {
	queriesPath, ok := os.LookupEnv(envQueriesDir)
	if !ok {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		queriesPath = filepath.Join(home, ".pjs", "queries")
	}

	if err := customq.LoadCustomQueries(root, afero.NewOsFs(), queriesPath, transposeImpl); err != nil {
		return err
	}
	return nil
}

func main() {
	if err := sigctx.With(root.ExecuteContext); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func transpose(cmd *cobra.Command, _ []string) error {
	byt, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}

	return transposeImpl(cmd.Context(), string(byt))
}

func transposeImpl(ctx context.Context, query string) error {
	var dsn string
	if *dsnF != `` {
		dsn = *dsnF
	}

	pool, err := internal.NewPool(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	type output map[string]interface{}
	if err = pjs.Transpose(ctx, pool, query, func(receivers []pjs.Receiver) error {
		out := make(output)
		for _, rec := range receivers {
			key := rec.Name
			if *typesF {
				key = fmt.Sprintf("%s (%T, %d)", key, rec.Val, rec.DataTypeOID)
			}
			out[key] = rec.Interface()
		}

		byt, err := json.Marshal(out)
		if err != nil {
			return fmt.Errorf("json.Marshal -- %w", err)
		}

		_, err = fmt.Println(string(byt))
		if err != nil {
			return fmt.Errorf("fmt.Println -- %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("pjs.Transpose, %w", err)
	}

	return nil
}
