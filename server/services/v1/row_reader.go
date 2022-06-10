package v1

import (
	"context"
	"encoding/json"
	"github.com/tigrisdata/tigris/schema"

	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
)

type Row struct {
	Key  []byte
	Data *internal.TableData
}

type RowReader interface {
	NextRow(ctx context.Context, row *Row) bool
	Err() error
}

type page struct {
	idx        int
	err        error
	resp       *pageResponse
	collection *schema.DefaultCollection
}

func (p *page) readRow(row *Row) bool {
	if p.err != nil {
		return false
	}

	for {
		document, more := p.resp.hits.GetDocument(p.idx)
		if !more {
			break
		}

		if document == nil {
			p.idx++
			continue
		}

		if p.err = UnpackSearchFields(document, p.collection); p.err != nil {
			return false
		}

		data, err := json.Marshal(*document)
		if err != nil {
			p.err = err
			return false
		}

		row.Key = []byte((*document)[searchID].(string))
		row.Data = &internal.TableData{RawData: data}
		break
	}
	hasNext := p.resp.hits.HasMoreHits(p.idx)
	p.idx++

	return hasNext
}

type SearchRowReader struct {
	pageNo     int
	perPage    int
	page       *page
	err        error
	store      search.Store
	filter     string
	result     *SearchResponse
	collection *schema.DefaultCollection
}

func MakeSearchRowReader(ctx context.Context, collection *schema.DefaultCollection, _ []read.Field, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	builder := qsearch.NewBuilder()
	searchFilter := builder.FromFilter(filters)

	s := &SearchRowReader{
		pageNo:     1,
		perPage:    5,
		store:      store,
		filter:     searchFilter,
		collection: collection,
	}

	return s, nil
}

func MakeSearchRowReaderUsingFilter(ctx context.Context, collection *schema.DefaultCollection, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	return MakeSearchRowReader(ctx, collection, nil, filters, store)
}

func (s *SearchRowReader) readPage(ctx context.Context) (bool, error) {
	result, err := s.store.Search(ctx, s.collection.SearchSchema.Name, s.filter, s.pageNo, s.perPage)
	if err != nil {
		return false, err
	}

	var hitsResp = NewHitsResponse()
	for _, r := range result {
		hitsResp.Append(r.Hits)
	}

	s.page = &page{
		idx:        0,
		collection: s.collection,
		resp: &pageResponse{
			hits:   hitsResp,
			facets: CreateFacetResponse(result[0].FacetCounts),
		},
	}

	return hitsResp.Count() < s.perPage, nil
}

func (s *SearchRowReader) NextRow(ctx context.Context, row *Row) bool {
	for {
		var lastPage bool
		if s.page == nil {
			if lastPage, s.err = s.readPage(ctx); s.err != nil {
				return false
			}
		}

		if s.page.readRow(row) {
			return true
		}

		if lastPage {
			return false
		}

		s.page = nil
		s.pageNo++
	}
}

func (s *SearchRowReader) GetFacet() *FacetResponse {
	return s.result.Facets
}

func (s *SearchRowReader) Err() error {
	if s.page != nil && s.page.err != nil {
		return s.page.err
	}
	return s.err
}

type DatabaseRowReader struct {
	idx        int
	tx         transaction.Tx
	ctx        context.Context
	err        error
	keys       []keys.Key
	kvIterator kv.Iterator
}

func MakeDatabaseRowReader(ctx context.Context, tx transaction.Tx, keys []keys.Key) (*DatabaseRowReader, error) {
	d := &DatabaseRowReader{
		idx:  0,
		tx:   tx,
		ctx:  ctx,
		keys: keys,
	}
	if d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx]); d.err != nil {
		return nil, d.err
	}

	return d, nil
}

func (d *DatabaseRowReader) NextRow(ctx context.Context, row *Row) bool {
	if d.err != nil {
		return false
	}

	for {
		var keyValue kv.KeyValue
		if d.kvIterator.Next(&keyValue) {
			row.Key = keyValue.FDBKey
			row.Data = keyValue.Data
			return true
		}
		if d.kvIterator.Err() != nil {
			d.err = d.kvIterator.Err()
			return false
		}

		d.idx++
		if d.idx == len(d.keys) {
			return false
		}

		d.kvIterator, d.err = d.readNextKey(d.ctx, d.keys[d.idx])
	}
}

func (d *DatabaseRowReader) readNextKey(ctx context.Context, key keys.Key) (kv.Iterator, error) {
	it, err := d.tx.Read(ctx, key)
	if ulog.E(err) {
		return nil, err
	}
	return it, nil

}

func (d *DatabaseRowReader) Err() error { return d.err }
