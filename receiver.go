package pjs

import (
	"github.com/jackc/pgtype"
)

type (
	// Receiver is a value receiver when reading postgres data.
	Receiver struct {
		Name        string
		DataTypeOID uint32
		Val         any
	}
)

func (r Receiver) Interface() any {
	switch r.DataTypeOID {
	case pgtype.ByteaOID:
		val := r.Val.(*pgtype.ByteaArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.CIDRArrayOID:
		val := r.Val.(*pgtype.CIDRArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.BoolArrayOID:
		val := r.Val.(*pgtype.BoolArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.Int2ArrayOID:
		val := r.Val.(*pgtype.Int2Array)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.Int4ArrayOID:
		val := r.Val.(*pgtype.Int4Array)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.TextArrayOID:
		val := r.Val.(*pgtype.TextArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.ByteaArrayOID:
		val := r.Val.(*pgtype.ByteaArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.BPCharArrayOID:
		val := r.Val.(*pgtype.BPCharArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.VarcharArrayOID:
		val := r.Val.(*pgtype.VarcharArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.Int8ArrayOID:
		val := r.Val.(*pgtype.Int8Array)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.Float4ArrayOID:
		val := r.Val.(*pgtype.Float4Array)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.Float8ArrayOID:
		val := r.Val.(*pgtype.Float8Array)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.ACLItemArrayOID:
		val := r.Val.(*pgtype.ACLItemArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.InetArrayOID:
		val := r.Val.(*pgtype.InetArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.TimestampArrayOID:
		val := r.Val.(*pgtype.TimestampArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.DateArrayOID:
		val := r.Val.(*pgtype.DateArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.TimestamptzArrayOID:
		val := r.Val.(*pgtype.TimestamptzArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.NumericArrayOID:
		val := r.Val.(*pgtype.NumericArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.UUIDArrayOID:
		val := r.Val.(*pgtype.UUIDArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.JSONBArrayOID:
		val := r.Val.(*pgtype.JSONBArray)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return []interface{}{}
		default:
			return val.Elements
		}
	case pgtype.NameOID:
		val := r.Val.(*pgtype.Name)
		switch val.Status {
		case pgtype.Null:
			return nil
		case pgtype.Undefined:
			return ``
		default:
			return val.String
		}
	default:
		return r.Val
	}
}
