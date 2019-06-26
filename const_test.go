package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewConstRunner(t *testing.T) {
	t.Run("invalid const data", func(t *testing.T) {
		invalidData := strs{"a", "b"}
		require.Panics(t, func() { ep.NewConstRunner(invalidData) })
	})

	t.Run("no input", func(t *testing.T) {
		r := ep.NewConstRunner(strs{"a"})
		res, err := eptest.Run(r)
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("single batch", func(t *testing.T) {
		r := ep.NewConstRunner(strs{"a"})
		inp := ep.NewDataset(integers{1, 2, 3, 4, 5, 6})
		res, err := eptest.Run(r, inp)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Width())
		require.Equal(t, inp.Len(), res.Len())
		require.Equal(t, []string{"(a)", "(a)", "(a)", "(a)", "(a)", "(a)"}, res.Strings())
	})

	t.Run("multiple batches", func(t *testing.T) {
		r := ep.NewConstRunner(strs{"a"})
		inp1 := ep.NewDataset(integers{1, 2, 3, 4, 5, 6})
		inp2 := ep.NewDataset(integers{7, 8})
		res, err := eptest.Run(r, inp1, inp2)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Width())
		require.Equal(t, inp1.Len()+inp2.Len(), res.Len())
		require.Equal(t, []string{"(a)", "(a)", "(a)", "(a)", "(a)", "(a)", "(a)", "(a)"}, res.Strings())
	})
}

func TestNewConstRunner_Equals(t *testing.T) {
	r1 := ep.NewConstRunner(strs{"a"})
	r2 := ep.NewConstRunner(strs{"a"})
	r3 := ep.NewConstRunner(strs{"b"})

	require.True(t, r1.Equals(r1))
	require.True(t, r1.Equals(r2))
	require.False(t, r1.Equals(r3))
}
