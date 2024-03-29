package scanner_test

import (
	"errors"
	"testing"

	"github.com/ingcr3at1on/pjs/pkg/scanner"
	"github.com/stretchr/testify/require"
)

func TestScanQueries(t *testing.T) {
	for name, tc := range map[string]struct {
		sql      string
		err      error
		expected []string
	}{
		"simple": {
			sql: `SELECT * FROM FOOBAR;`,
			expected: []string{
				`SELECT * FROM FOOBAR;`,
			},
		},
		"still-simple": {
			sql: `SELECT * FROM FOOBAR;
SELECT * FROM BARFOO;`,
			expected: []string{
				`SELECT * FROM FOOBAR;`,
				`SELECT * FROM BARFOO;`,
			},
		},
		"still-simple-inline": {
			sql: `SELECT * FROM FOOBAR;SELECT * FROM BARFOO;`,
			expected: []string{
				`SELECT * FROM FOOBAR;`,
				`SELECT * FROM BARFOO;`,
			},
		},
		"simple-multi-line": {
			sql: `SELECT * FROM FOOBAR
WHERE t = 'x';`,
			expected: []string{
				`SELECT * FROM FOOBAR
WHERE t = 'x';`,
			},
		},
		"do-func": {
			sql: `do $$
	SELECT * FROM FOOBAR;
END $$;`,
			expected: []string{
				`do $$
	SELECT * FROM FOOBAR;
END $$;`,
			},
		},
		"complex": {
			sql: `SELECT * FROM FOOBAR;
do $$
	SELECT * FROM BARFOO;
end $$;`,
			expected: []string{
				`SELECT * FROM FOOBAR;`,
				`do $$
	SELECT * FROM BARFOO;
end $$;`,
			},
		},
	} {
		t.Run(name, func(tt *testing.T) {
			batch, err := scanner.ScanQueries(tc.sql)
			if tc.err == nil {
				require.NoError(tt, err)
				require.Equal(tt, len(tc.expected), batch.Len())
				// FIXME: would be nice to validate the query shape again.
			} else {
				require.True(tt, errors.Is(err, tc.err))
			}
		})
	}
}
