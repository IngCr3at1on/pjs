package jq

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Panel struct {
	in  *in
	out *output
}

func New() *Panel {
	out := newOut("no output")
	p := Panel{
		in:  newIn(out),
		out: out,
	}

	return &p
}

func (p *Panel) Init() tea.Cmd {
	return p.in.Init()
}

func (p *Panel) Update(message tea.Msg) tea.Cmd {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	cmd = p.in.Update(message)
	cmds = append(cmds, cmd)

	cmd = p.out.Update(message)
	cmds = append(cmds, cmd)

	return tea.Batch(cmds...)
}

func (p *Panel) View() string {
	return lipgloss.JoinVertical(
		lipgloss.Top, p.in.View(), p.out.View())
}

func (p *Panel) Focus() tea.Cmd {
	return p.in.Focus()
}

func (p *Panel) Blur() {
	p.in.Blur()
}

func (p *Panel) Focused() bool {
	return p.in.Focused()
}

func (p *Panel) SetData(data map[string]any) {
	p.out.SetData(data)
}
