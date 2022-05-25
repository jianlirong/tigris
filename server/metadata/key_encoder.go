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

package metadata

import (
	"bytes"
	"fmt"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
)

var (
	userTableKeyPrefix = []byte("data")
)

// Encoder is used to encode/decode values of the Key.
type Encoder interface {
	// EncodeTableName returns encoded bytes which are formed by combining namespace, database, and collection.
	EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error)
	// EncodeIndexName returns encoded bytes for the index name
	EncodeIndexName(idx *schema.Index) []byte
	// EncodeKey returns encoded bytes of the key which will be used to store the values in fdb. The Key return by this
	// method has two parts,
	//   - tableName: This is set with an encoding of namespace, database and collection id.
	//   - IndexParts: This has the index identifier and value(s) associated with a single or composite index. This is appended
	//	   to the table name to form the Key. The first element of this list is the dictionary encoding of index type key
	//	   information i.e. whether the index is pkey, etc. The remaining elements are values for this index.
	EncodeKey(encodedTable []byte, idx *schema.Index, idxParts []interface{}) (keys.Key, error)

	// EncodeSearchTableName encodes the table name that we are using for search collection. It is simply string
	// delimited using "-"
	EncodeSearchTableName(ns string, db string, coll string) string

	// DecodeTableName is used to decode the key stored in FDB and extract namespace name, database name and collection name.
	DecodeTableName(tableName []byte) (string, string, string, bool)
	DecodeIndexName(indexName []byte) uint32
}

// NewEncoder creates Dictionary encoder to encode keys.
func NewEncoder(mgr *TenantManager) Encoder {
	return &DictKeyEncoder{
		mgr: mgr,
	}
}

type DictKeyEncoder struct {
	mgr *TenantManager
}

func (d *DictKeyEncoder) EncodeTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) ([]byte, error) {
	if db == nil {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "database is missing")
	}
	if coll == nil {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "collection is missing")
	}
	return d.encodedTableName(ns, db, coll), nil
}

func (d *DictKeyEncoder) EncodeSearchTableName(namespace string, database string, collection string) string {
	return fmt.Sprintf("%s-%s-%s", namespace, database, collection)
}

func (d *DictKeyEncoder) EncodeIndexName(idx *schema.Index) []byte {
	return d.encodedIdxName(idx)
}

func (d *DictKeyEncoder) EncodeKey(encodedTable []byte, idx *schema.Index, idxParts []interface{}) (keys.Key, error) {
	if idx == nil {
		return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "index is missing")
	}

	encodedIdxName := d.encodedIdxName(idx)

	var remainingKeyParts []interface{}
	remainingKeyParts = append(remainingKeyParts, encodedIdxName)
	remainingKeyParts = append(remainingKeyParts, idxParts...)

	return keys.NewKey(encodedTable, remainingKeyParts...), nil
}

func (d *DictKeyEncoder) encodedTableName(ns Namespace, db *Database, coll *schema.DefaultCollection) []byte {
	var appendTo []byte
	appendTo = append(appendTo, userTableKeyPrefix...)
	appendTo = append(appendTo, encoding.UInt32ToByte(ns.Id())...)
	appendTo = append(appendTo, encoding.UInt32ToByte(db.id)...)
	appendTo = append(appendTo, encoding.UInt32ToByte(coll.Id)...)
	return appendTo
}

func (d *DictKeyEncoder) encodedIdxName(idx *schema.Index) []byte {
	return encoding.UInt32ToByte(idx.Id)
}

func (d *DictKeyEncoder) DecodeTableName(tableName []byte) (string, string, string, bool) {
	if len(tableName) < 16 || !bytes.Equal(tableName[0:4], userTableKeyPrefix) {
		return "", "", "", false
	}

	nsId := encoding.ByteToUInt32(tableName[4:8])
	dbId := encoding.ByteToUInt32(tableName[8:12])
	collId := encoding.ByteToUInt32(tableName[12:16])

	return d.mgr.GetTableNameFromId(nsId, dbId, collId)
}

func (d *DictKeyEncoder) DecodeIndexName(indexName []byte) uint32 {
	return encoding.ByteToUInt32(indexName)
}
