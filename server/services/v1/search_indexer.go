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

package v1

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
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	"time"
)

var (
	ErrSearchIndexingFailed = fmt.Errorf("failed to index documents")
)

type SearchIndexer struct {
	searchStore search.Store
	encoder     metadata.Encoder
}

func NewSearchIndexer(searchStore search.Store, encoder metadata.Encoder) *SearchIndexer {
	return &SearchIndexer{
		searchStore: searchStore,
		encoder:     encoder,
	}
}

func (i *SearchIndexer) OnPostCommit(ctx context.Context, eventListener kv.EventListener) error {
	events := eventListener.GetEvents()
	fmt.Println("event s", events)
	for _, event := range events {
		var err error
		ns, db, coll, ok := i.encoder.DecodeTableName(event.Table)
		if !ok {
			continue
		}

		searchTableName := i.encoder.EncodeSearchTableName(ns, db, coll)
		tableData, err := internal.Decode(event.Data)
		if err != nil {
			return err
		}

		searchKey, err := CreateSearchKey(event.Table, event.Key)
		if err != nil {
			return err
		}

		var action string
		switch event.Op {
		case kv.InsertEvent, kv.ReplaceEvent:
			action = "upsert"
		case kv.UpdateEvent:
			action = "update"
		}

		tableData.RawData, err = jsonparser.Set(tableData.RawData, searchKey, "id")
		if err != nil {
			return err
		}

		fmt.Println("searchTableName ", searchTableName, event.Op, " searched key", string(searchKey))
		for attempt := 0; attempt < 5; attempt++ {
			fmt.Println(string(tableData.RawData))
			reader := bytes.NewReader(tableData.RawData)
			err = i.searchStore.IndexDocuments(ctx, searchTableName, reader, search.IndexDocumentsOptions{
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

func (i *SearchIndexer) OnCommit(context.Context, transaction.Tx, kv.EventListener) error { return nil }

func (i *SearchIndexer) OnRollback(context.Context, kv.EventListener) {}

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
