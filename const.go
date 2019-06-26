package ep

import (
	"context"
	"github.com/panoplyio/ep/compare"
)

var _ = registerGob(&constt{})

// NewConstRunner returns a Runner that duplicates pre-defined data according
// to input size. d is expected to be single row data
func NewConstRunner(d Data) Runner {
	if d.Len() != 1 {
		panic("invalid const runner creation. please pass single-row data")
	}
	return &constt{d}
}

type constt struct {
	Data
}

func (r *constt) Equals(other interface{}) bool {
	o, ok := other.(*constt)
	if !ok {
		return false
	}
	c, err := r.Data.Compare(o.Data)
	return err == nil && c[0] == compare.Equal
}
func (r *constt) Returns() []Type { return []Type{r.Data.Type()} }
func (r *constt) Run(_ context.Context, inp, out chan Dataset) error {
	for data := range inp {
		res, _ := r.run(data)
		out <- res
	}
	return nil
}
func (r *constt) run(data Dataset) (Dataset, error) {
	res := r.Duplicate(data.Len())
	return NewDataset(res), nil
}
func (r *constt) BatchFunction() BatchFunction {
	return r.run
}
func (r *constt) Scopes() StringsSet {
	return nil
}
