package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/cmd/internal"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea"
)

func main() {
	if err := start(); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func start() error {
	// FIXME: context should be the same as what's being passed to the background, etc.
	pool, err := internal.NewPool(context.Background(), "")
	if err != nil {
		return err
	}
	defer pool.Close()

	// FIXME: add show-types flag.
	model := bubbletea.New(pool, false)
	p := tea.NewProgram(model)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("p.Start -- %w", err)
	}

	return nil
}
