package bubbles

import (
	"sync/atomic"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
)

// OutputPane is a viewport that will dynamically resize itself.
type OutputPane struct {
	ready       atomic.Bool
	m           viewport.Model
	calcWidth   util.CalcF
	calcHeight  util.CalcF
	placeholder string
}

func NewOutputPane(calcWidth, calcHeight util.CalcF, placeholder string) *OutputPane {
	return &OutputPane{
		calcWidth:   calcWidth,
		calcHeight:  calcHeight,
		placeholder: placeholder,
	}
}

func (p *OutputPane) Update(message tea.Msg) (*OutputPane, tea.Cmd) {
	switch msg := message.(type) {
	case tea.WindowSizeMsg:
		width := p.calcWidth(msg.Width)
		height := p.calcHeight(msg.Height)

		if !p.ready.Load() {
			p.m = viewport.New(width, height)
			p.m.SetContent(p.placeholder)
			p.ready.Store(true)
		} else {
			p.m.Width = width
			p.m.Height = height
		}
	}

	var cmd tea.Cmd
	p.m, cmd = p.m.Update(message)

	return p, cmd
}

func (p *OutputPane) View() string {
	return p.m.View()
}

func (p *OutputPane) SetContent(content string) {
	p.m.SetContent(content)
}
