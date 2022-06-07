package search

import (
	"github.com/tigrisdata/tigris/query/filter"
)

type Query struct{}

type Spec struct {
	Query  Query
	Filter []filter.Filter
}

type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

func (f *Builder) FromFilter(filters []filter.Filter) string {
	var str string
	for i, f := range filters {
		str += f.ToSearchFilter()
		if i < len(filters)-1 {
			str += "&&"
		}
	}
	return str
}
