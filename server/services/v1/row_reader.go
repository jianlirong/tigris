package v1

import (
	"context"
	"encoding/json"
	"github.com/tigrisdata/tigris/internal"
	qsearch "github.com/tigrisdata/tigris/query/search"
	"github.com/tigrisdata/tigris/store/search"
	tsApi "github.com/typesense/typesense-go/typesense/api"

	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
)

/**

RowReader(filter)
	-> GetKeysOnly(filter)
	-> ReadDocuments(filter)

	-> Parse Filter
	-> 		if able to Build Key
				-> Return keys
	-> 		else
				-> Create SearchReader
				-> Read documents using the supplied filter
				-> Create keys
				-> Returns keys

1. Update
	- Build Key
		- If successful directly read from FDB (read and update)
	- Else
		- r := SearchRowReader{}
		- key := r.NextKey()
		- Read from Search Engine
		- Create Key from Payload
		- Perform KV operation in FDB (read and update)

	Update API
		- Filter

2. Delete
	- Build Key
		- If successful directly delete in FDB
	- Else
		- r := SearchRowReader{searchEngine[SearchRow]}
		- key := r.NextKey()
		- Read from Search Engine
		- Create Key from Payload
		- delete in FDB

3. Read
	- Build Key
		- If successful directly read from FDB
		- r := DatabaseRowReader{DatabaseEngine[DatabaseRow]}
	- Else
		- Read from Search Engine
		- r := RowReader{DatabaseEngine[DatabaseRow]}

4. Search
		- r := RowReader{searchEngine[SearchRow]}
		- key := r.NextRow()
*/

type Row struct {
	Key  []byte
	Data *internal.TableData
}

type SearchResult struct {
	Hits        *[]tsApi.SearchResultHit
	FacetCounts *[]tsApi.FacetCounts
}

type RowReader interface {
	NextRow(row *Row) bool
	Err() error
}

type SearchRowReader struct {
	idx    int
	err    error
	result *SearchResult
}

func MakeSearchRowReader(ctx context.Context, table string, _ []read.Field, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	searchReqBuilder := qsearch.Builder{}
	searchFilter := searchReqBuilder.BuildUsingFilters(filters)

	result, err := store.Search(ctx, table, searchFilter)
	if err != nil {
		return nil, err
	}

	var hits []tsApi.SearchResultHit
	for _, r := range result {
		if r.Hits != nil {
			hits = append(hits, *r.Hits...)
		}
	}
	var s = &SearchResult{
		Hits:        &hits,
		FacetCounts: result[0].FacetCounts,
	}

	return &SearchRowReader{
		idx:    0,
		result: s,
	}, nil
}

func MakeSearchRowReaderUsingFilter(ctx context.Context, table string, filters []filter.Filter, store search.Store) (*SearchRowReader, error) {
	return MakeSearchRowReader(ctx, table, nil, filters, store)
}

func (s *SearchRowReader) NextRow(row *Row) bool {
	for s.idx < len(*s.result.Hits) {
		document := (*s.result.Hits)[s.idx].Document
		if document == nil {
			s.idx++
			continue
		}

		data, err := json.Marshal(*document)
		if err != nil {
			s.err = err
			return false
		}
		row.Key = []byte((*document)["id"].(string))
		row.Data = &internal.TableData{
			RawData: data,
		}
		break
	}
	hasNext := s.idx < len(*s.result.Hits)
	s.idx++

	return hasNext
}

func (s *SearchRowReader) GetFacet() []tsApi.FacetCounts {
	if s.result.FacetCounts != nil {
		return *s.result.FacetCounts
	}

	return nil
}

func (s *SearchRowReader) Err() error {
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

func (d *DatabaseRowReader) NextRow(row *Row) bool {
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
