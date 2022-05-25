package search

import "github.com/tigrisdata/tigris/query/filter"

type Builder struct{}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) BuildUsingFilters(filters []filter.Filter) string {
	var str string
	for i, f := range filters {
		str += f.ToSearchFilter()
		if i < len(filters)-1 {
			str += "&&"
		}
	}
	return str
}
