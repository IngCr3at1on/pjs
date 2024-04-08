package bubbletea

import (
	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles/db"
	"github.com/ingcr3at1on/pjs/pkg/bubbletea/bubbles/jq"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	keymap = struct {
		change, quit key.Binding
	}

	model struct {
		db     db.Panel
		jq     *jq.Panel
		keymap keymap
	}
)

var _ tea.Model = new(model)

func New(pool *pgxpool.Pool, showTypes bool) model {
	jqPanel := jq.New()

	m := model{
		keymap: keymap{
			change: key.NewBinding(
				key.WithKeys("tab"),
				key.WithHelp("tab", "change input windows")),
			quit: key.NewBinding(
				key.WithKeys("esc"),
				key.WithHelp("esc", "quit")),
		},
		db: db.New(pool, showTypes, jqPanel.SetData),
		jq: jqPanel,
	}

	return m
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.db.Init(), m.jq.Init())
}

func (m model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := message.(type) {
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keymap.quit):
			return m, tea.Quit
		case key.Matches(msg, m.keymap.change):
			switch {
			case m.db.Focused():
				cmd = m.jq.Focus()
				m.db.Blur()
				cmds = append(cmds, cmd)
			case m.jq.Focused():
				cmd = m.db.Focus()
				m.jq.Blur()
				cmds = append(cmds, cmd)
			}
		}
	}

	m.db, cmd = m.db.Update(message)
	cmds = append(cmds, cmd)

	cmd = m.jq.Update(message)
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	return lipgloss.JoinHorizontal(
		lipgloss.Left, m.db.View(), m.jq.View())
}
