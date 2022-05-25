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
	"bytes"
	"context"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"time"
)

var (
	ErrSearchIndexingFailed = fmt.Errorf("failed to index documents")
)

type Executor struct {
	searchStore search.Store
	encoder     metadata.Encoder
}

func NewExecutor(searchStore search.Store, encoder metadata.Encoder) *Executor {
	return &Executor{
		searchStore: searchStore,
		encoder:     encoder,
	}
}

func (e *Executor) WrapContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, kv.SearchListenerCtxKey{}, &listener{})
}

func (e *Executor) OnCommit(ctx context.Context) error {
	l := kv.GetSearchListener(ctx)
	fmt.Println("OnCommit called on listener ", l.(*listener).operations)

	for _, o := range l.(*listener).operations {
		var err error
		ns, db, coll, ok := e.encoder.DecodeTableName(o.table)
		if !ok {
			continue
		}

		searchTableName := e.encoder.EncodeSearchTableName(ns, db, coll)
		tableData, err := internal.Decode(o.data)
		if err != nil {
			return err
		}

		searchKey, err := CreateSearchKey(o.table, o.key)
		if err != nil {
			return err
		}

		var action string
		switch o.opType {
		case kv.InsertOp, kv.ReplaceOp:
			action = "upsert"
		case kv.UpdateOp:
			action = "update"
		}

		tableData.RawData, err = jsonparser.Set(tableData.RawData, searchKey, "id")
		if err != nil {
			return err
		}

		fmt.Println("searchTableName ", searchTableName, o.opType, " searched key", string(searchKey))
		for i := 0; i < 5; i++ {
			fmt.Println(string(tableData.RawData))
			reader := bytes.NewReader(tableData.RawData)
			err = e.searchStore.IndexDocuments(ctx, searchTableName, reader, search.IndexDocumentsOptions{
				Action:    action,
				BatchSize: 1,
			})

			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func CreateSearchKey(table []byte, fdbKey []byte) ([]byte, error) {
	sb := subspace.FromBytes(table)
	tp, err := sb.Unpack(fdb.Key(fdbKey))
	if err != nil {
		return nil, err
	}

	// ToDo: add a pkey check here
	if tp[0] != schema.PrimaryKeyIndexName {
		// this won't work as tp[0] is dictionary encoded value of PrimaryKeyIndexName
	}

	// the zeroth entry represents index key name
	tp = tp[1:]
	if len(tp) == 1 {
		// simply marshal it if it is single primary key
		switch key := tp[0].(type) {
		case int32, int64, int:
			// we need to convert numeric to string
			return []byte(fmt.Sprintf(`"%d"`, key)), nil
		}
		return jsoniter.Marshal(tp[0])
	} else {
		// for composite there is no easy way, pack it and then marshal it
		return jsoniter.Marshal(tp.Pack())
	}
}

func (e *Executor) OnRollback(ctx context.Context) error {
	l := kv.GetSearchListener(ctx)
	l.(*listener).operations = nil
	return nil
}

type Op struct {
	opType string
	table  []byte
	key    []byte
	data   []byte
}

type listener struct {
	operations []Op
}

func (l *listener) OnSet(op string, table []byte, key []byte, data []byte) {
	fmt.Println("OnSet called on listener")
	l.operations = append(l.operations, Op{
		opType: op,
		table:  table,
		key:    key,
		data:   data,
	})
}

func (l *listener) OnClearRange(op string, table []byte, lKey []byte, rKey []byte) {}

func (l *listener) OnCommit(_ *fdb.Transaction) error { return nil }

func (l *listener) OnCancel() {}
