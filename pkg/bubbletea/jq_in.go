package bubbletea

import (
	"sync/atomic"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/itchyny/gojq"
)

type jqIn struct {
	ready atomic.Bool
	m     textinput.Model
	out   *jqOut
}

func newJqIn(out *jqOut) *jqIn {
	p := jqIn{
		m:   textinput.New(),
		out: out,
	}

	p.m.Placeholder = "type jq here"

	return &p
}

func (*jqIn) Init() tea.Cmd {
	return textinput.Blink
}

func (p *jqIn) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		p.m.Width = util.HalfWidth(msg.Width)

		if !p.ready.Load() {
			p.m.Reset()
			p.ready.Store(true)
		}
	}

	//if val := p.m.Value(); val != "" {
	if exp, err := gojq.Parse(p.m.Value()); err == nil {
		p.out.setExp(exp)
	}
	//}

	var cmd tea.Cmd
	p.m, cmd = p.m.Update(message)

	return p, cmd
}

func (p *jqIn) View() string {
	return p.m.View()
}

func (p *jqIn) Focus() tea.Cmd {
	return p.m.Focus()
}

func (p *jqIn) Blur() {
	p.m.Blur()
}

func (p *jqIn) Focused() bool {
	return p.m.Focused()
}
