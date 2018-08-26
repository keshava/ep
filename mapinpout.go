package ep

import (
	"context"
)

var _ = registerGob(mapInpToOut{})

// MapInpToOut wraps around a runner such that it maps the input to that runner
// to all of the outputs generated by it. Essentially, each input row is
// duplicated multiple times to match the number of output rows (for that
// particular input). For example, if a single input row generates N output
// rows, the input will be appear N times in the result (even when N is zero),
// alongside the N output rows.
//
// The result dataset is a concatenation of the inputs and their corresponding
// outputs. This is especially useful in cases where a single input row might
// generate a disproprotionally large output, and we want to be able to
// correlate which input row generated which output rows. In such cases, each
// result row will include the inputs and outputs joined horizontally (input on
// the left, output on the right)
//
// NOTE that MapInpToOut is prohibitive in terms of performance due to excessive
// memory allocations (see comment in Run()). It should only be used for cases
// where (a) the input data is significantly smaller than the output,
// (b) offline processes where performance is less of a concern or (c) when the
// data is already sliced thinly (few rows per batch).
//
// NOTE that the nested runner cannot be composed of one-time runners like
// exchange (Scatter, Broadcast, etc.), which cannot be executed more than once.
func MapInpToOut(r Runner) Runner {
	return &mapInpToOut{r}
}

type mapInpToOut struct{ Runner }

// Returns all of the inputs types joined by all of the output types of the
// nested runner
func (r *mapInpToOut) Returns() []Type {
	return append([]Type{Wildcard}, r.Runner.Returns()...)
}

// Run has no way of knowning when the output of one input ends and another
// begins. In order to correlate the individual inputs to the outputs they
// generated, Run has to run each input row through the nested runner to
// completion. This means that the input is first sliced row by row, which
// involves many allocations, and then the nested runner is executed to collect
// all of the outputs generated, which is basically a separate goroutine per
// input row (sequentially, not in parallel). For this reason, MapInpToOut
// should be used with care, either when the data is already sliced thinly, or
// for places where performance is less of a concern. In general - if there's a
// different way to avoid using MapInpToOut, it's possibly preferrable.
func (r *mapInpToOut) Run(ctx context.Context, inp, out chan Dataset) error {
	var err error
	for data := range inp {
		for i := 0; i < data.Len(); i++ {
			d := data.Slice(i, i+1).(Dataset) // one row at a time.
			innerOut := make(chan Dataset)
			innerInp := make(chan Dataset, 1)
			innerInp <- d
			close(innerInp)

			// run the inner runner to completion with the single input. this is
			// because otherwise we will not be able to map every output row to
			// input row that generated it. There's no 1-to-1 correlation, or
			// any guarantee on the number or size of batches being processed.
			go func() {
				defer close(innerOut)
				err = r.Runner.Run(ctx, innerInp, innerOut)
			}()

			for res := range innerOut {
				// no error; length of both sides is the same.
				res, _ := d.Duplicate(res.Len()).(Dataset).Expand(res)
				out <- res
			}

			if err != nil {
				return err
			}
		}
	}

	return nil
}