package bubbletea

import (
	"math"

	"github.com/ingcr3at1on/pjs"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/util"
	"github.com/ingcr3at1on/pjs/pkg/pgjq"
)

type (
	dbOut struct {
		showTypes   bool
		placeholder string
		*bubbles.OutputPane
		callback Callback
	}

	Callback func(data map[string]any)
)

func newDbOut(placeholder string, showTypes bool, dataCallback Callback) dbOut {
	return dbOut{
		showTypes:   showTypes,
		placeholder: placeholder,
		OutputPane:  bubbles.NewOutputPane(util.HalfWidth, dbOutHeight, placeholder),
		callback:    dataCallback,
	}
}

//func (p dbOut) Update(message tea.Msg) (dbOut, tea.Cmd) {
//	var cmd tea.Cmd
//	p.OutputPane, cmd = p.OutputPane.Update(message)
//	return p, cmd
//}

func (p dbOut) transpose(receivers []pjs.Receiver) error {
	m, str, err := pgjq.Convert(receivers, p.showTypes)
	if err != nil {
		return err
	}

	p.SetContent(str)
	p.callback(m)
	return nil
}

func (p dbOut) reset() {
	p.SetContent(p.placeholder)
}

func dbOutHeight(total int) int {
	h := float64(total) * .75
	return int(math.Round(h))
}
