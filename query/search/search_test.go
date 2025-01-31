// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/schema"
)

func TestSearchBuilder(t *testing.T) {
	js := []byte(`{"a": 4, "$and": [{"int_value":1}, {"string_value1": "shoe"}]}`)
	f := filter.NewFactory([]*schema.Field{
		{FieldName: "a", DataType: schema.Int64Type},
		{FieldName: "int_value", DataType: schema.Int64Type},
		{FieldName: "string_value1", DataType: schema.StringType},
	})
	filters, err := f.Factorize(js)
	require.NoError(t, err)
	require.Len(t, filters, 2)

	b := Builder{}
	require.Equal(t, "a:=4&&int_value:=1&&string_value1:=shoe", b.FromFilter(filters))
}
