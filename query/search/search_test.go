package search

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/schema"
	"testing"
)

func TestSearchBuilder(t *testing.T) {
	js := []byte(`{"f1": 10, "f2": 10}`)
	js = []byte(`{"a": 4, "$and": [{"int_value":1}, {"string_value1": "shoe"}]}`)
	f := filter.NewFactory([]*schema.Field{
		{FieldName: "a", DataType: schema.Int64Type},
		{FieldName: "int_value", DataType: schema.Int64Type},
		{FieldName: "string_value1", DataType: schema.StringType},
	})
	filters, err := f.Build(js)
	require.NoError(t, err)
	require.Len(t, filters, 2)

	s := Builder{}
	fmt.Println(s.BuildUsingFilters(filters))
}
