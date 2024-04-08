package jq

import (
	"strings"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/itchyny/gojq"
)

type output struct {
	*bubbles.OutputPane

	mux  sync.RWMutex
	exp  *gojq.Query
	data map[string]any
}

func newOut(placeholder string) *output {
	return &output{
		OutputPane: bubbles.NewOutputPane(util.HalfWidth, jqOutHeight, placeholder),
	}
}

func (p *output) Update(message tea.Msg) tea.Cmd {
	p.marshal()

	var cmd tea.Cmd
	p.OutputPane, cmd = p.OutputPane.Update(message)
	return cmd
}

func (p *output) SetData(data map[string]any) {
	p.mux.Lock()
	p.data = data
	p.mux.Unlock()
}

func (p *output) setExp(exp *gojq.Query) {
	p.mux.Lock()
	p.exp = exp
	p.mux.Unlock()
}

func (p *output) marshal() {
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

func (p *output) marshalQuery() {
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
