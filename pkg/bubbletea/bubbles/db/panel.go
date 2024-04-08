package db

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Panel struct {
	in  *in
	out output
}

func New(pool *pgxpool.Pool, showTypes bool, dataCallback Callback) Panel {
	out := newOut("no output", showTypes, dataCallback)
	p := Panel{
		in:  newIn(pool, &out),
		out: out,
	}

	return p
}

func (p Panel) Init() tea.Cmd {
	return p.in.Init()
}

func (p Panel) Update(message tea.Msg) (Panel, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	cmd = p.in.Update(message)
	cmds = append(cmds, cmd)

	p.out, cmd = p.out.Update(message)
	cmds = append(cmds, cmd)

	return p, tea.Batch(cmds...)
}

func (p Panel) View() string {
	return lipgloss.JoinVertical(
		lipgloss.Top, p.in.View(), p.out.View())
}

func (p Panel) Focus() tea.Cmd {
	return p.in.Focus()
}

func (p Panel) Blur() {
	p.in.Blur()
}

func (p Panel) Focused() bool {
	return p.in.Focused()
}
