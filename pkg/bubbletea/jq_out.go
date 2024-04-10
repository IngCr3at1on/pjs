package bubbletea

import (
	"strings"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/itchyny/gojq"
)

type jqOut struct {
	*bubbles.OutputPane

	mux  sync.RWMutex
	exp  *gojq.Query
	data map[string]any
}

var _ tea.Model = (*jqOut)(nil)

func newJqOut(placeholder string) *jqOut {
	return &jqOut{
		OutputPane: bubbles.NewOutputPane(util.HalfWidth, jqOutHeight, placeholder),
	}
}

func (p *jqOut) Init() tea.Cmd {
	return nil
}

func (p *jqOut) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	p.marshal()

	var (
		cmd  tea.Cmd
		pane tea.Model
	)
	pane, cmd = p.OutputPane.Update(message)
	if op, ok := pane.(*bubbles.OutputPane); ok {
		p.OutputPane = op
	}

	return p, cmd
}

func (p *jqOut) View() string {
	return p.OutputPane.View()
}

func (p *jqOut) SetData(data map[string]any) {
	p.mux.Lock()
	p.data = data
	p.mux.Unlock()
}

func (p *jqOut) setExp(exp *gojq.Query) {
	p.mux.Lock()
	p.exp = exp
	p.mux.Unlock()
}

func (p *jqOut) marshal() {
	p.mux.RLock()
	if p.data != nil {
		if p.exp != nil {
			p.marshalQuery()
		} else {
			byt, err := gojq.Marshal(p.data)
			if err != nil {
				p.SetContent(err.Error())
			} else {
				p.SetContent(string(byt))
			}
		}
	}
	p.mux.RUnlock()
}

func (p *jqOut) marshalQuery() {
	iter := p.exp.Run(p.data)
	var content []string
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}

		if err, ok := v.(error); ok {
			p.SetContent(err.Error())
			return
		}

		byt, err := gojq.Marshal(v)
		if err != nil {
			p.SetContent(err.Error())
			return
		}

		content = append(content, string(byt))
	}

	p.SetContent(strings.Join(content, "\n"))
}

func jqOutHeight(total int) int {
	return total - 1
}
