package bubbletea

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
	dbKeymap = struct {
		doQuery, clear key.Binding
	}

	dbIn struct {
		ctx    context.Context
		ready  atomic.Bool
		m      textarea.Model
		keymap dbKeymap

		pool *pgxpool.Pool
		out  *dbOut
	}
)

var _ tea.Model = new(dbIn)

func newDbIn(ctx context.Context, pool *pgxpool.Pool, out *dbOut) *dbIn {
	p := dbIn{
		ctx: ctx,
		m:   textarea.New(),
		keymap: dbKeymap{
			doQuery: key.NewBinding(
				key.WithKeys(tea.KeyCtrlS.String()),
				key.WithHelp("ctrl+s", "send psql query")),
			clear: key.NewBinding(
				key.WithKeys(tea.KeyCtrlC.String()),
				key.WithHelp("ctrl+c", "clear query")),
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

func (*dbIn) Init() tea.Cmd {
	return textarea.Blink
}

func (p *dbIn) Update(message tea.Msg) (tea.Model, tea.Cmd) {
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

	return p, tea.Batch(cmds...)
}

func (p *dbIn) handleSizeMessage(msg tea.WindowSizeMsg) {
	p.m.SetWidth(util.HalfWidth(msg.Width))
	p.m.SetHeight(dbInHeight(msg.Height))

	if !p.ready.Load() {
		p.m.Reset()
		p.ready.Store(true)
	}
}

func (p *dbIn) handleKeyMessage(msg tea.KeyMsg) {
	switch {
	case key.Matches(msg, p.keymap.doQuery):
		if err := p.transpose(p.m.Value()); err != nil {
			p.out.SetContent(err.Error())
		}
	case key.Matches(msg, p.keymap.clear):
		p.m.Reset()
		p.out.reset()
	}
}

func (p *dbIn) transpose(query string) error {
	// FIXME: get the context from the application...
	return pjs.Transpose(context.Background(), p.pool, query, p.out.transpose)
}

func (p *dbIn) View() string {
	//var b strings.Builder
	//b.WriteString(p.m.View() + "\n")
	//
	//h := p.keymap.doQuery.Help()
	//b.WriteString(h.Key + " " + h.Desc + ", ")
	//
	//p.keymap.clear.Help()
	//b.WriteString(h.Key + " " + h.Desc)
	//return b.String()
	return p.m.View()
}

func (p *dbIn) Focus() tea.Cmd {
	return p.m.Focus()
}

func (p *dbIn) Blur() {
	p.m.Blur()
}

func (p *dbIn) Focused() bool {
	return p.m.Focused()
}

func dbInHeight(total int) int {
	h := float64(total) * .25
	return int(math.Round(h))
}
