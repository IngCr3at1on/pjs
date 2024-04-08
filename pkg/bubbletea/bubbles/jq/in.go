package jq

import (
	"sync/atomic"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/itchyny/gojq"
)

type in struct {
	ready atomic.Bool
	m     textinput.Model
	out   *output
}

func newIn(out *output) *in {
	p := in{
		m:   textinput.New(),
		out: out,
	}

	p.m.Placeholder = "type jq here"

	return &p
}

func (*in) Init() tea.Cmd {
	return textinput.Blink
}

func (p *in) Update(message tea.Msg) tea.Cmd {
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

	return cmd
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
