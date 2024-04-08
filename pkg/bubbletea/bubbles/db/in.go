package db

import (
	"context"
	"math"
	"sync/atomic"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	keymap = struct {
		doQuery, clearInput key.Binding
	}

	in struct {
		ready  atomic.Bool
		m      textarea.Model
		keymap keymap

		pool *pgxpool.Pool
		out  *output
	}
)

func newIn(pool *pgxpool.Pool, out *output) *in {
	p := in{
		m: textarea.New(),
		keymap: keymap{
			doQuery: key.NewBinding(
				key.WithKeys(tea.KeyCtrlS.String()),
				key.WithHelp("ctrl+s", "send psql query")),
			clearInput: key.NewBinding(
				key.WithKeys(tea.KeyCtrlC.String()),
				key.WithHelp("ctrl+c", "clear query input")),
		},
		pool: pool,
		out:  out,
	}

	p.m.Focus()
	p.m.Prompt = "| "
	p.m.ShowLineNumbers = false
	p.m.Placeholder = "type pqsl here"

	return &p
}

func (*in) Init() tea.Cmd {
	return textarea.Blink
}

func (p *in) Update(message tea.Msg) tea.Cmd {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		p.handleSizeMessage(msg)
	case tea.KeyMsg:
		p.handleKeyMessage(msg)
	}

	p.m, cmd = p.m.Update(message)
	cmds = append(cmds, cmd)

	return tea.Batch(cmds...)
}

func (p *in) handleSizeMessage(msg tea.WindowSizeMsg) {
	p.m.SetWidth(util.HalfWidth(msg.Width))
	p.m.SetHeight(dbInHeight(msg.Height))

	if !p.ready.Load() {
		p.m.Reset()
		p.ready.Store(true)
	}
}

func (p *in) handleKeyMessage(msg tea.KeyMsg) {
	switch {
	case key.Matches(msg, p.keymap.doQuery):
		p.transpose(p.m.Value())
	case key.Matches(msg, p.keymap.clearInput):
		p.m.Reset()
	}
}

func (p *in) transpose(query string) error {
	// FIXME: get the context from the application...
	return pjs.Transpose(context.Background(), p.pool, query, p.out.transpose)
}

func (p *in) View() string {
	return p.m.View()
}

func (p *in) Focus() tea.Cmd {
	return p.m.Focus()
}

func (p *in) Blur() {
	p.m.Blur()
}

func (p *in) Focused() bool {
	return p.m.Focused()
}

func dbInHeight(total int) int {
	h := float64(total) * .25
	return int(math.Round(h))
}
