package ep

import (
	"context"
	"sync"
)

var _ = registerGob(pipeline([]Runner{}))

// Pipeline returns a vertical composite pipeline runner where the output of
// any one stream is passed as input to the next
func Pipeline(runners ...Runner) Runner {
	if len(runners) == 0 {
		panic("at least 1 runner is required for pipelining")
	}

	// flatten nested pipelines. note we should examine only first level, as any
	// pre-created pipeline was already flatten during its creation
	var flat pipeline
	for _, r := range runners {
		p, isPipe := r.(pipeline)
		if isPipe {
			flat = append(flat, p...)
		} else {
			flat = append(flat, r)
		}
	}

	// filter out passThrough runners as they don't affect the pipe
	filtered := flat[:0]
	for _, r := range flat {
		if r != passThroughSingleton {
			filtered = append(filtered, r)
		}
	}

	if len(filtered) == 0 {
		// all runners were filtered, pipe should simply return its output as is
		return PassThrough()
	} else if len(filtered) == 1 {
		return filtered[0] // only one runner left, no need for a pipeline
	}

	if cmp, ok := createComposeRunner(runners); ok {
		return cmp
	}
	return filtered
}

type pipeline []Runner

func (rs pipeline) Equals(other interface{}) bool {
	otherP, ok := other.(pipeline)
	if !ok || len(rs) != len(otherP) {
		return false
	}

	for i, r := range rs {
		if !r.Equals(otherP[i]) {
			return false
		}
	}

	return true
}

func (rs pipeline) Run(ctx context.Context, inp, out chan Dataset) (err error) {
	ctx = initError(ctx)
	// choose first error out from all errors
	errs := make([]error, len(rs))
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		for _, e := range errs {
			if e != nil && e != ErrIgnorable && e != errOnPeer {
				err = e
				break
			}
		}
	}()

	ctx, cancel := context.WithCancel(ctx)
	inp = wrapInpWithCancel(ctx, inp)

	// run all (except last one) runners, piping the output from each runner to its next
	lastIndex := len(rs) - 1
	for i := 0; i < lastIndex; i++ {
		// middle chan is the output from the current runner and the input to
		// the next
		middle := make(chan Dataset)

		wg.Add(1)
		go func(i int, inp, middle chan Dataset) {
			defer wg.Done()
			Run(ctx, rs[i], inp, middle, cancel, &errs[i])
		}(i, inp, middle)

		// input to the next channel is the output from the current one
		inp = middle
	}

	// allow previous runner to continue and notify cancellation
	defer drain(inp)

	// cancel all runners when we're done - just in case some are still running. This
	// might happen if the top of the pipeline ends before the bottom of the pipeline
	defer cancel()

	// block until last runner completion
	errs[lastIndex] = rs[lastIndex].Run(ctx, inp, out)
	return
}

func wrapInpWithCancel(ctx context.Context, inp chan Dataset) chan Dataset {
	newInp := make(chan Dataset)
	go func() {
		defer drain(inp)
		defer close(newInp)
		for {
			select {
			case <-ctx.Done():
				return
			case data, open := <-inp:
				if !open {
					return
				}
				newInp <- data
			}
		}
	}()
	return newInp
}

// The implementation isn't trivial because it has to account for Wildcard types
// which indicate that the actual types should be retrieved from the input, thus
// when a Wildcard is found in the To runner, this function will replace it with
// the entire return types of the From runner (which might be another pair -
// calling this function recursively).
// see Runner & Wildcard
func (rs pipeline) Returns() []Type {
	last := len(rs) - 1
	return returnsOne(last, rs.getIReturns) // start with the last one.
}

func (rs pipeline) getIReturns(i int) returns { return rs[i] }

// Args returns the arguments expected by the first runner in this pipeline.
// If that runner is an instance of RunnerArgs, its Args() result will be returned.
// Otherwise, Wildcard type is returned.
func (rs pipeline) Args() []Type {
	runnerArgs, ok := rs[0].(RunnerArgs)
	if ok {
		return runnerArgs.Args()
	}
	return []Type{Wildcard}
}

func returnsOne(j int, get func(int) returns) []Type {
	res := get(j).Returns()
	if j == 0 {
		return res
	}

	// check for wildcards, and replace as needed. Walk backwards to allow
	// adding types in-place without messing the iteration
	for i := len(res) - 1; i >= 0; i-- {
		if w, isWildcard := res[i].(*wildcardType); isWildcard {
			// wildcard found - replace it with the types from the previous
			// runner (which might also contain Wildcards)
			prev := returnsOne(j-1, get)
			prev = prev[:len(prev)-w.CutFromTail]
			if w.Idx != nil {
				// wildcard for a specific column in the input
				prev = prev[*w.Idx : *w.Idx+1]
			}
			res = append(res[:i], append(prev, res[i+1:]...)...)
		}
	}
	return res
}

func (rs pipeline) Filter(keep []bool) {
	last := rs[len(rs)-1]
	if f, isFilterable := last.(FilterRunner); isFilterable {
		f.Filter(keep)
	}
}

func (rs pipeline) Scopes() StringsSet {
	scopes := make(StringsSet)
	for _, r := range rs {
		if r, ok := r.(ScopesRunner); ok {
			scopes.AddAll(r.Scopes())
		}
	}
	return scopes
}

func (rs pipeline) Push(toPush ScopesRunner) bool {
	for _, r := range rs {
		// try to push as earlier as we can
		if r, ok := r.(PushRunner); ok {
			isPushed := r.Push(toPush)
			if isPushed {
				return isPushed
			}
		}
	}
	return false
}

func (rs pipeline) ApproxSize() int {
	for i := len(rs) - 1; i >= 0; i-- {
		r := rs[i]
		if approxSizer, ok := r.(ApproxSizer); ok {
			size := approxSizer.ApproxSize()
			if size != UnknownSize {
				return size
			}
		}
	}
	return UnknownSize
}
