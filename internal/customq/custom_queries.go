package customq

// TODO: add tests...

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/hashicorp/hcl/v2/hclsimple"
	"github.com/ingcr3at1on/x/lazyfstools"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type (
	queries struct {
		Queries []query `hcl:"query,block"`

		parent *cobra.Command
	}

	query struct {
		Use         string   `hcl:"use,label"`
		Description string   `hcl:"description,optional"`
		Template    string   `hcl:"template"`
		Args        []string `hcl:"args,optional"`

		t *template.Template
	}
)

func LoadCustomQueries(parent *cobra.Command, afs afero.Fs, path string, transposeImpl func(ctx context.Context, query string) error) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("filepath.Abs(%s) -- %w", path, err)
	}

	return lazyfstools.Walk(afs, abs,
		fileProcessor(parent, transposeImpl), dirProcessor(parent))
}

func fileProcessor(parent *cobra.Command, transposeImpl func(ctx context.Context, query string) error) lazyfstools.ProcessFunc {
	return func(afs afero.Fs, path string, info fs.FileInfo) error {
		if strings.HasSuffix(info.Name(), ".json") || strings.HasSuffix(info.Name(), ".hcl") {
			byt, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("os.ReadFile(%s) -- %w", path, err)
			}
			q := queries{
				parent: parent,
			}
			if err = q.loadCustomQuery(info.Name(), byt, transposeImpl); err != nil {
				return err
			}
		}

		return nil
	}
}

func dirProcessor(parent *cobra.Command) lazyfstools.ProcessFunc {
	return func(afs afero.Fs, path string, info fs.FileInfo) error {
		nc := cobra.Command{
			Use:           info.Name(),
			SilenceErrors: true,
			SilenceUsage:  true,
			PreRunE:       cobra.NoArgs,
		}

		parent.AddCommand(&nc)
		return nil
	}
}

func (qs *queries) loadCustomQuery(filename string, byt []byte, transposeImpl func(ctx context.Context, query string) error) error {
	if err := hclsimple.Decode(filename, byt, nil, qs); err != nil {
		return fmt.Errorf("hclsimple.Decode(%s) -- %w", filename, err)
	}

	for _, q := range qs.Queries {
		q.t = template.New(filename)
		var err error
		q.t, err = q.t.Parse(q.Template)
		if err != nil {
			return fmt.Errorf("error reading template from %s -- %w", filename, err)
		}

		qs.parent.AddCommand(&cobra.Command{
			Use:           q.Use,
			Long:          q.Description,
			SilenceErrors: true,
			PreRunE: func(cmd *cobra.Command, args []string) error {
				if len(args) != len(q.Args) {
					return cmd.Usage()
				}
				return nil
			},
			RunE: func(cmd *cobra.Command, args []string) error {
				m := make(map[string]string)
				for n, argKey := range q.Args {
					m[argKey] = args[n]
				}

				var buf bytes.Buffer
				if err := q.t.Execute(&buf, m); err != nil {
					return fmt.Errorf("q.t.Execute -- %w", err)
				}

				return transposeImpl(cmd.Context(), buf.String())
			},
		})
	}

	return nil
}
