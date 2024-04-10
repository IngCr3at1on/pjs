package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/cmd/internal"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea"
	"github.com/ingcr3at1on/x/sigctx"
	"github.com/spf13/cobra"
)

// TODO:
// 		* implement a version of stdin support based on the original CLI app.
//		* add custom queries, requires the previous.
//  	* add environment configs.
// 		* consider trying to fix jq support when showTypes is set (for now it's disabled, a legacy debug feature anyway).

const (
	dsnFK = `dsn`
	//typesFK = `show-types`
)

var (
	dsnF *string
	//typesF *bool

	root = &cobra.Command{
		Use:           "pjs",
		SilenceErrors: true,
		SilenceUsage:  true,
		PreRunE:       cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return start(cmd.Context())
		},
	}
)

func init() {
	flags := root.PersistentFlags()
	dsnF = flags.StringP(dsnFK, `d`, ``, `a dsn string`)
	//typesF = flags.BoolP(typesFK, `t`, false, `show types`)
}

func main() {
	if err := sigctx.With(root.ExecuteContext); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func start(ctx context.Context) error {
	var dsn string
	if *dsnF != `` {
		dsn = *dsnF
	}

	pool, err := internal.NewPool(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	model := bubbletea.New(ctx, pool, false)
	p := tea.NewProgram(model, tea.WithoutSignalHandler(), tea.WithContext(ctx))

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("p.Start -- %w", err)
	}

	return nil
}
