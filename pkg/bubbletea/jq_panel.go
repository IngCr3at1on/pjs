package bubbletea

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
)

type jqPanel struct {
	panel bubbles.VerticalPanel
	out   *jqOut
	input *jqIn
}

var _ tea.Model = (*jqPanel)(nil)

func newJqPanel() *jqPanel {
	out := newJqOut("no output")
	input := newJqIn(out)
	return &jqPanel{
		panel: bubbles.NewVerticalPanel(input, out),
		out:   out,
		input: input,
	}
}

func (p *jqPanel) Init() tea.Cmd {
	return p.input.Init()
}

func (p *jqPanel) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	return p.panel.Update(message)
}

func (p *jqPanel) View() string {
	return p.panel.View()
}

func (p *jqPanel) Focused() bool {
	return p.input.Focused()
}

func (p *jqPanel) Focus() tea.Cmd {
	return p.input.Focus()
}

func (p *jqPanel) Blur() {
	p.input.Blur()
}

func (p *jqPanel) SetData(data map[string]any) {
	p.out.SetData(data)
}
