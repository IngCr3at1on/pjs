package bubbletea

import (
	"context"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/jackc/pgx/v5/pgxpool"
)

type (
	keymap = struct {
		change, quit key.Binding
	}

	model struct {
		db     dbPanel
		jq     *jqPanel
		keymap keymap
	}
)

var _ tea.Model = new(model)

func New(ctx context.Context, pool *pgxpool.Pool, showTypes bool) model {
	jqPanel := newJqPanel()

	m := model{
		keymap: keymap{
			change: key.NewBinding(
				key.WithKeys("tab"),
				key.WithHelp("tab", "change input windows")),
			quit: key.NewBinding(
				key.WithKeys("esc"),
				key.WithHelp("esc", "quit")),
		},
		db: newDBPanel(ctx, pool, showTypes, jqPanel.SetData),
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

	var db tea.Model
	db, cmd = m.db.Update(message)
	if panel, ok := db.(dbPanel); ok {
		m.db = panel
	}
	cmds = append(cmds, cmd)

	var jq tea.Model
	jq, cmd = m.jq.Update(message)
	if panel, ok := jq.(*jqPanel); ok {
		m.jq = panel
	}
	cmds = append(cmds, cmd)

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	return lipgloss.JoinHorizontal(
		lipgloss.Left, m.db.View(), m.jq.View())
}
