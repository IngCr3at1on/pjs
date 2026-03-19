package bubbletea

import (
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/ingcr3at1on/pjs"
	"github.com/jackc/pgtype"
	"github.com/stretchr/testify/require"
)

func TestDbOut_Transpose_AppendsRowsToOutput(t *testing.T) {
	panel := newDbOut("placeholder", false, func(data map[string]any) {})

	// Ensure viewport model is initialized.
	_, _ = panel.Update(tea.WindowSizeMsg{Width: 120, Height: 40})

	rec := func(v int32) []pjs.Receiver {
		return []pjs.Receiver{{
			Name: "n",
			Val: &pgtype.Int4{
				Int:    v,
				Status: pgtype.Present,
			},
		}}
	}

	require.NoError(t, panel.transpose(rec(1)))
	require.NoError(t, panel.transpose(rec(2)))

	view := panel.View()
	require.Contains(t, view, `"n":1`, "first result should remain visible after second transpose")
	require.Contains(t, view, `"n":2`, "second result should be appended to output")
}
