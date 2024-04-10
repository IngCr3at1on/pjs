package bubbles

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type VerticalPanel struct {
	input  tea.Model
	output tea.Model
}

var _ tea.Model = (*VerticalPanel)(nil)

func NewVerticalPanel(input tea.Model, output tea.Model) VerticalPanel {
	return VerticalPanel{
		input:  input,
		output: output,
	}
}

func (p VerticalPanel) Init() tea.Cmd {
	return p.input.Init()
}

func (p VerticalPanel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds = make([]tea.Cmd, 0, 2)
	)

	p.input, cmd = p.input.Update(message)
	cmds = append(cmds, cmd)

	p.output, cmd = p.output.Update(message)
	cmds = append(cmds, cmd)

	return p, tea.Batch(cmds...)
}

func (p VerticalPanel) View() string {
	return lipgloss.JoinVertical(
		lipgloss.Top, p.input.View(), p.output.View())
}
