package bubbletea

import (
	"context"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModel_Update(t *testing.T) {
	t.Run("focus change", func(tt *testing.T) {
		m := New(context.Background(), nil, false)
		var tm tea.Model = m

		cmd := tm.Init()
		msg := cmd()
		b, ok := msg.(tea.BatchMsg)
		require.True(tt, ok)
		assert.Len(tt, b, 2)

		require.True(tt, m.db.Focused())
		require.False(tt, m.jq.Focused())

		tm, cmd = tm.Update(tea.KeyMsg{
			Type: tea.KeyTab,
		})
		msg = cmd()
		b, ok = msg.(tea.BatchMsg)
		require.True(tt, ok)
		assert.Len(tt, b, 1) // 3 commands but 2 are nil so Batch skipped them.
		assert.False(tt, m.db.Focused())
		assert.True(tt, m.jq.Focused())
	})
}
