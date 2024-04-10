package bubbletea

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbPanel struct {
	bubbles.VerticalPanel
	input *dbIn
}

func newDBPanel(ctx context.Context, pool *pgxpool.Pool, showTypes bool, dataCallback Callback) dbPanel {
	out := newDbOut("no output", showTypes, dataCallback)
	input := newDbIn(ctx, pool, &out)
	return dbPanel{
		VerticalPanel: bubbles.NewVerticalPanel(input, out),
		input:         input,
	}
}

func (p dbPanel) Focused() bool {
	return p.input.Focused()
}

func (p dbPanel) Focus() tea.Cmd {
	return p.input.Focus()
}

func (p dbPanel) Blur() {
	p.input.Blur()
}
