package pgjq

import (
	"encoding/json"
	"fmt"

	"github.com/ingcr3at1on/pjs"
)

func Convert(receivers []pjs.Receiver, showTypes bool) (map[string]any, string, error) {
	out := make(map[string]any)
	for _, rec := range receivers {
		key := rec.Name
		if showTypes {
			key = fmt.Sprintf("%s (%T, %d)", key, rec.Val, rec.DataTypeOID)
		}

		out[key] = rec.Interface()
	}

	byt, err := json.Marshal(out)
	if err != nil {
		return nil, ``, err
	}

	return out, string(byt), json.Unmarshal(byt, &out)
}
