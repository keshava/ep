package ep_test

import (
	"github.com/panoplyio/ep"
	"github.com/panoplyio/ep/eptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConster(t *testing.T) {
	t.Run("invalid const data", func(t *testing.T) {
		invalidData := strs{"a", "b"}
		require.Panics(t, func() { ep.Conster(invalidData) })
	})

	t.Run("no input", func(t *testing.T) {
		r := ep.Conster(strs{"a"})
		res, err := eptest.Run(r)
		require.NoError(t, err)
		require.Nil(t, res)
	})

	t.Run("single batch", func(t *testing.T) {
		r := ep.Conster(strs{"a"})
		inp := ep.NewDataset(integers{1, 2, 3, 4, 5, 6})
		res, err := eptest.Run(r, inp)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Width())
		require.Equal(t, inp.Len(), res.Len())
	})

	t.Run("multiple batches batch", func(t *testing.T) {
		r := ep.Conster(strs{"a"})
		inp1 := ep.NewDataset(integers{1, 2, 3, 4, 5, 6})
		inp2 := ep.NewDataset(integers{7, 8})
		res, err := eptest.Run(r, inp1, inp2)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, res.Width())
		require.Equal(t, inp1.Len()+inp2.Len(), res.Len())
	})
}
