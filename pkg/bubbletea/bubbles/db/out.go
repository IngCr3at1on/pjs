package db

import (
	"math"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/ingcr3at1on/pjs/pkg/pgjq"
)

type (
	output struct {
		showTypes bool
		*bubbles.OutputPane
		callback Callback
	}

	Callback func(data map[string]any)
)

func newOut(placeholder string, showTypes bool, dataCallback Callback) output {
	return output{
		showTypes:  showTypes,
		OutputPane: bubbles.NewOutputPane(util.HalfWidth, dbOutHeight, placeholder),
		callback:   dataCallback,
	}
}

func (p output) Update(message tea.Msg) (output, tea.Cmd) {
	var cmd tea.Cmd
	p.OutputPane, cmd = p.OutputPane.Update(message)
	return p, cmd
}

func (p output) transpose(receivers []pjs.Receiver) error {
	m, str, err := pgjq.Convert(receivers, p.showTypes)
	if err != nil {
		return err
	}

	p.SetContent(str)
	p.callback(m)
	return nil
}

func dbOutHeight(total int) int {
	h := float64(total) * .75
	return int(math.Round(h))
}
