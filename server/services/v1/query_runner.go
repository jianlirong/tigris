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

	"github.com/buger/jsonparser"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/cdc"
	"github.com/tigrisdata/tigris/internal"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/query/filter"
	"github.com/tigrisdata/tigris/query/read"
	"github.com/tigrisdata/tigris/query/update"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/tigrisdata/tigris/value"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// QueryRunner is responsible for executing the current query and return the response
type QueryRunner interface {
	Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error)
}

// QueryRunnerFactory is responsible for creating query runners for different queries
type QueryRunnerFactory struct {
	txMgr   *transaction.Manager
	encoder metadata.Encoder
	cdcMgr  *cdc.Manager
}

// NewQueryRunnerFactory returns QueryRunnerFactory object
func NewQueryRunnerFactory(txMgr *transaction.Manager, encoder metadata.Encoder, cdcMgr *cdc.Manager) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		txMgr:   txMgr,
		encoder: encoder,
		cdcMgr:  cdcMgr,
	}
}

func (f *QueryRunnerFactory) GetInsertQueryRunner(r *api.InsertRequest) *InsertQueryRunner {
	return &InsertQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetReplaceQueryRunner(r *api.ReplaceRequest) *ReplaceQueryRunner {
	return &ReplaceQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetUpdateQueryRunner(r *api.UpdateRequest) *UpdateQueryRunner {
	return &UpdateQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
		req:             r,
	}
}

func (f *QueryRunnerFactory) GetDeleteQueryRunner(r *api.DeleteRequest) *DeleteQueryRunner {
	return &DeleteQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
		req:             r,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner
func (f *QueryRunnerFactory) GetStreamingQueryRunner(r *api.ReadRequest, streaming Streaming) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
		req:             r,
		streaming:       streaming,
	}
}

func (f *QueryRunnerFactory) GetCollectionQueryRunner() *CollectionQueryRunner {
	return &CollectionQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
	}
}

func (f *QueryRunnerFactory) GetDatabaseQueryRunner() *DatabaseQueryRunner {
	return &DatabaseQueryRunner{
		BaseQueryRunner: NewBaseQueryRunner(f.encoder, f.cdcMgr),
	}
}

type BaseQueryRunner struct {
	encoder metadata.Encoder
	cdcMgr  *cdc.Manager
}

func NewBaseQueryRunner(encoder metadata.Encoder, cdcMgr *cdc.Manager) *BaseQueryRunner {
	return &BaseQueryRunner{
		encoder: encoder,
		cdcMgr:  cdcMgr,
	}
}

func (runner *BaseQueryRunner) GetDatabase(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, dbName string) (*metadata.Database, error) {
	if tx.Context().GetStagedDatabase() != nil {
		// this means that some DDL operation has modified the database object, then we need to perform all the operations
		// on this staged database.
		return tx.Context().GetStagedDatabase().(*metadata.Database), nil
	}

	// otherwise, simply read from the in-memory cache/disk.
	db, err := tenant.GetDatabase(ctx, tx, dbName)
	if err != nil {
		return nil, err
	}
	if db == nil {
		// database not found
		return nil, api.Errorf(codes.NotFound, "database doesn't exist '%s'", dbName)
	}

	return db, nil
}

func (runner *BaseQueryRunner) GetCollections(db *metadata.Database, collName string) (*schema.DefaultCollection, error) {
	collection := db.GetCollection(collName)
	if collection == nil {
		return nil, api.Errorf(codes.NotFound, "collection doesn't exist '%s'", collName)
	}

	return collection, nil
}

func (runner *BaseQueryRunner) extractIndexParts(userDefinedKeys []*schema.Field, doc []byte) ([]interface{}, error) {
	var appendTo []interface{}
	for _, v := range userDefinedKeys {
		jsonVal, _, _, err := jsonparser.Get(doc, v.FieldName)
		if err != nil {
			return nil, api.Errorf(codes.InvalidArgument, errors.Wrapf(err, "missing index key column(s) '%s'", v.FieldName).Error())
		}

		val, err := value.NewValue(v.DataType, jsonVal)
		if err != nil {
			return nil, err
		}
		appendTo = append(appendTo, val.AsInterface())
	}
	if len(appendTo) == 0 {
		return nil, ulog.CE("missing index key column(s)")
	}
	return appendTo, nil
}

func (runner *BaseQueryRunner) insertOrReplace(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection, documents [][]byte, insert bool) (*internal.Timestamp, error) {
	var err error
	var ts = internal.NewTimestamp()
	for _, doc := range documents {
		var deserializedDoc map[string]interface{}
		dec := jsoniter.NewDecoder(bytes.NewReader(doc))
		dec.UseNumber()
		if err = dec.Decode(&deserializedDoc); ulog.E(err) {
			return nil, err
		}
		if err = coll.Validate(deserializedDoc); err != nil {
			// schema validation failed
			return nil, err
		}

		indexParts, err := runner.extractIndexParts(coll.Indexes.PrimaryKey.Fields, doc)
		if ulog.E(err) {
			return nil, err
		}

		var key keys.Key
		if key, err = runner.encoder.EncodeKey(tenant.GetNamespace(), db, coll, coll.Indexes.PrimaryKey, indexParts); ulog.E(err) {
			return nil, err
		}

		tableData := internal.NewTableDataWithTS(ts, nil, doc)
		if insert {
			err = tx.Insert(ctx, key, tableData)
		} else {
			err = tx.Replace(ctx, key, tableData)
		}
		if err != nil {
			return nil, err
		}
	}
	return ts, err
}

func (runner *BaseQueryRunner) buildKeysUsingFilter(tenant *metadata.Tenant, db *metadata.Database, coll *schema.DefaultCollection, reqFilter []byte) ([]keys.Key, error) {
	filterFactory := filter.NewFactory(coll.Fields)
	filters, err := filterFactory.Build(reqFilter)
	if err != nil {
		return nil, err
	}

	primaryKeyIndex := coll.Indexes.PrimaryKey
	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(func(indexParts ...interface{}) (keys.Key, error) {
		return runner.encoder.EncodeKey(tenant.GetNamespace(), db, coll, primaryKeyIndex, indexParts)
	}))
	iKeys, err := kb.Build(filters, coll.Indexes.PrimaryKey.Fields)
	if err != nil {
		return nil, err
	}

	return iKeys, nil
}

type InsertQueryRunner struct {
	*BaseQueryRunner

	req *api.InsertRequest
}

func (runner *InsertQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	coll, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	ts, err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), true)
	if err != nil {
		if err == kv.ErrDuplicateKey {
			return nil, ctx, api.Errorf(codes.AlreadyExists, err.Error())
		}

		return nil, ctx, err
	}
	return &Response{
		status:    InsertedStatus,
		createdAt: ts,
	}, ctx, nil
}

type ReplaceQueryRunner struct {
	*BaseQueryRunner

	req *api.ReplaceRequest
}

func (runner *ReplaceQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	coll, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	ts, err := runner.insertOrReplace(ctx, tx, tenant, db, coll, runner.req.GetDocuments(), false)
	if err != nil {
		return nil, ctx, err
	}
	return &Response{
		status:    ReplacedStatus,
		createdAt: ts,
	}, ctx, nil
}

type UpdateQueryRunner struct {
	*BaseQueryRunner

	req *api.UpdateRequest
}

func (runner *UpdateQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	var ts = internal.NewTimestamp()
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, ctx, err
	}

	var factory *update.FieldOperatorFactory
	factory, err = update.BuildFieldOperators(runner.req.Fields)
	if err != nil {
		return nil, ctx, err
	}

	for _, fieldOperators := range factory.FieldOperators {
		v, err := fieldOperators.DeserializeDoc()
		if err != nil {
			return nil, ctx, err
		}

		if err = collection.Validate(v); err != nil {
			// schema validation failed
			return nil, ctx, err
		}
	}

	modifiedCount := int32(0)
	for _, key := range iKeys {
		// decode the fields now
		modified := int32(0)
		if modified, err = tx.Update(ctx, key, func(existing *internal.TableData) (*internal.TableData, error) {
			merged, er := factory.MergeAndGet(existing.RawData)
			if er != nil {
				return nil, er
			}

			// ToDo: may need to change the schema version
			return internal.NewTableDataWithTS(existing.CreatedAt, ts, merged), nil
		}); ulog.E(err) {
			return nil, ctx, err
		}
		modifiedCount += modified
	}

	return &Response{
		status:        UpdatedStatus,
		updatedAt:     ts,
		modifiedCount: modifiedCount,
	}, ctx, err
}

type DeleteQueryRunner struct {
	*BaseQueryRunner

	req *api.DeleteRequest
}

func (runner *DeleteQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	var ts = internal.NewTimestamp()
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, ctx, err
	}

	for _, key := range iKeys {
		if err = tx.Delete(ctx, key); ulog.E(err) {
			return nil, ctx, err
		}
	}

	return &Response{
		status:    DeletedStatus,
		updatedAt: ts,
	}, ctx, nil
}

// StreamingQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion
type StreamingQueryRunner struct {
	*BaseQueryRunner

	req       *api.ReadRequest
	streaming Streaming
}

// Run is responsible for running/executing the query
func (runner *StreamingQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	db, err := runner.GetDatabase(ctx, tx, tenant, runner.req.GetDb())
	if err != nil {
		return nil, ctx, err
	}

	ctx = runner.cdcMgr.WrapContext(ctx, db.Name())

	collection, err := runner.GetCollections(db, runner.req.GetCollection())
	if err != nil {
		return nil, ctx, err
	}

	fieldFactory, err := read.BuildFields(runner.req.GetFields())
	if ulog.E(err) {
		return nil, ctx, err
	}

	if filter.IsFullCollectionScan(runner.req.GetFilter()) {
		table := runner.encoder.EncodeTableName(tenant.GetNamespace(), db, collection)
		resp, err := runner.iterateCollection(ctx, tx, table, fieldFactory)
		if err != nil {
			log.Debug().Str("db", db.Name()).Str("coll", collection.Name).Bytes("encoding", table).Err(err).Msg("full scan")
		}

		return resp, ctx, err
	}

	// or this is a read request that needs to be streamed after filtering the keys.
	iKeys, err := runner.buildKeysUsingFilter(tenant, db, collection, runner.req.Filter)
	if err != nil {
		return nil, ctx, err
	}

	resp, err := runner.iterateKeys(ctx, tx, iKeys, fieldFactory)
	if err != nil {
		log.Debug().Str("db", db.Name()).Str("coll", collection.Name).Err(err).Msg("iterate keys")
	}
	return resp, ctx, err
}

// iterateCollection is used to scan the entire collection.
func (runner *StreamingQueryRunner) iterateCollection(ctx context.Context, tx transaction.Tx, table []byte, fieldFactory *read.FieldFactory) (*Response, error) {
	var totalResults int64 = 0
	if err := runner.iterate(ctx, tx, keys.NewKey(table), fieldFactory, &totalResults); err != nil {
		return nil, err
	}

	return &Response{}, nil
}

// iterateKeys is responsible for building keys from the filter and then executing the query. A key could be a primary
// key or an index key.
func (runner *StreamingQueryRunner) iterateKeys(ctx context.Context, tx transaction.Tx, iKeys []keys.Key, fieldFactory *read.FieldFactory) (*Response, error) {
	var totalResults int64 = 0
	for _, key := range iKeys {
		if err := runner.iterate(ctx, tx, key, fieldFactory, &totalResults); err != nil {
			return nil, err
		}
	}

	return &Response{}, nil
}

func (runner *StreamingQueryRunner) iterate(ctx context.Context, tx transaction.Tx, key keys.Key, fieldFactory *read.FieldFactory, totalResults *int64) error {
	it, err := tx.Read(ctx, key)
	if ulog.E(err) {
		return err
	}

	var limit int64 = 0
	if runner.req.GetOptions() != nil {
		limit = runner.req.GetOptions().Limit
	}

	var row kv.KeyValue
	for it.Next(&row) {
		if limit > 0 && limit <= *totalResults {
			return nil
		}

		newValue, err := fieldFactory.Apply(row.Data.RawData)
		if ulog.E(err) {
			return err
		}
		var createdAt, updatedAt *timestamppb.Timestamp
		if row.Data.CreatedAt != nil {
			createdAt = row.Data.CreatedAt.GetProtoTS()
		}
		if row.Data.UpdatedAt != nil {
			updatedAt = row.Data.UpdatedAt.GetProtoTS()
		}

		if err := runner.streaming.Send(&api.ReadResponse{
			Data: newValue,
			Metadata: &api.ResponseMetadata{
				CreatedAt: createdAt,
				UpdatedAt: updatedAt,
			},
			ResumeToken: row.FDBKey,
		}); ulog.E(err) {
			return err
		}

		*totalResults++
	}

	return it.Err()
}

type CollectionQueryRunner struct {
	*BaseQueryRunner

	dropReq           *api.DropCollectionRequest
	listReq           *api.ListCollectionsRequest
	createOrUpdateReq *api.CreateOrUpdateCollectionRequest
	describeReq       *api.DescribeCollectionRequest
}

func (runner *CollectionQueryRunner) SetCreateOrUpdateCollectionReq(create *api.CreateOrUpdateCollectionRequest) {
	runner.createOrUpdateReq = create
}

func (runner *CollectionQueryRunner) SetDropCollectionReq(drop *api.DropCollectionRequest) {
	runner.dropReq = drop
}

func (runner *CollectionQueryRunner) SetListCollectionReq(list *api.ListCollectionsRequest) {
	runner.listReq = list
}

func (runner *CollectionQueryRunner) SetDescribeCollectionReq(describe *api.DescribeCollectionRequest) {
	runner.describeReq = describe
}

func (runner *CollectionQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	if runner.dropReq != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.dropReq.GetDb())
		if err != nil {
			return nil, ctx, err
		}

		if tx.Context().GetStagedDatabase() == nil {
			// do not modify the actual database object yet, just work on the clone
			db = db.Clone()
			tx.Context().StageDatabase(db)
		}

		if err = tenant.DropCollection(ctx, tx, db, runner.dropReq.GetCollection()); err != nil {
			return nil, ctx, err
		}

		return &Response{
			status: DroppedStatus,
		}, ctx, nil
	} else if runner.createOrUpdateReq != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.createOrUpdateReq.GetDb())
		if err != nil {
			return nil, ctx, err
		}

		if db.GetCollection(runner.createOrUpdateReq.GetCollection()) != nil && runner.createOrUpdateReq.OnlyCreate {
			// check if onlyCreate is set and if yes then return an error if collection already exist
			return nil, ctx, api.Errorf(codes.AlreadyExists, "collection already exist")
		}

		schFactory, err := schema.Build(runner.createOrUpdateReq.GetCollection(), runner.createOrUpdateReq.GetSchema())
		if err != nil {
			return nil, ctx, err
		}

		if tx.Context().GetStagedDatabase() == nil {
			// do not modify the actual database object yet, just work on the clone
			db = db.Clone()
			tx.Context().StageDatabase(db)
		}

		if err = tenant.CreateCollection(ctx, tx, db, schFactory); err != nil {
			if err == kv.ErrDuplicateKey {
				// this simply means, concurrently CreateCollection is called,
				return nil, ctx, api.Errorf(codes.Aborted, "concurrent create collection request, aborting")
			}
			return nil, ctx, err
		}

		return &Response{
			status: CreatedStatus,
		}, ctx, nil
	} else if runner.listReq != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.listReq.GetDb())
		if err != nil {
			return nil, ctx, err
		}

		collectionList := db.ListCollection()
		var collections = make([]*api.CollectionInfo, len(collectionList))
		for i, c := range collectionList {
			collections[i] = &api.CollectionInfo{
				Collection: c.GetName(),
			}
		}
		return &Response{
			Response: &api.ListCollectionsResponse{
				Collections: collections,
			},
		}, ctx, nil
	} else if runner.describeReq != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.describeReq.GetDb())
		if err != nil {
			return nil, ctx, err
		}

		coll, err := runner.GetCollections(db, runner.describeReq.GetCollection())
		if err != nil {
			return nil, ctx, err
		}
		return &Response{
			Response: &api.DescribeCollectionResponse{
				Collection: coll.Name,
				Metadata:   &api.CollectionMetadata{},
				Schema:     coll.Schema,
			},
		}, ctx, nil
	}

	return &Response{}, ctx, api.Errorf(codes.Unknown, "unknown request path")
}

type DatabaseQueryRunner struct {
	*BaseQueryRunner

	drop     *api.DropDatabaseRequest
	create   *api.CreateDatabaseRequest
	list     *api.ListDatabasesRequest
	describe *api.DescribeDatabaseRequest
}

func (runner *DatabaseQueryRunner) SetCreateDatabaseReq(create *api.CreateDatabaseRequest) {
	runner.create = create
}

func (runner *DatabaseQueryRunner) SetDropDatabaseReq(drop *api.DropDatabaseRequest) {
	runner.drop = drop
}

func (runner *DatabaseQueryRunner) SetListDatabaseReq(list *api.ListDatabasesRequest) {
	runner.list = list
}

func (runner *DatabaseQueryRunner) SetDescribeDatabaseReq(describe *api.DescribeDatabaseRequest) {
	runner.describe = describe
}

func (runner *DatabaseQueryRunner) Run(ctx context.Context, tx transaction.Tx, tenant *metadata.Tenant) (*Response, context.Context, error) {
	if runner.drop != nil {
		exist, err := tenant.DropDatabase(ctx, tx, runner.drop.GetDb())
		if err != nil {
			return nil, ctx, err
		}
		if !exist {
			return nil, ctx, api.Errorf(codes.NotFound, "database doesn't exist '%s'", runner.drop.GetDb())
		}

		return &Response{
			status: DroppedStatus,
		}, ctx, nil
	} else if runner.create != nil {
		exist, err := tenant.CreateDatabase(ctx, tx, runner.create.GetDb())
		if err != nil {
			return nil, ctx, err
		}
		if exist {
			return nil, ctx, api.Errorf(codes.AlreadyExists, "database already exist")
		}

		return &Response{
			status: CreatedStatus,
		}, ctx, nil
	} else if runner.list != nil {
		databaseList := tenant.ListDatabases(ctx, tx)

		var databases = make([]*api.DatabaseInfo, len(databaseList))
		for i, l := range databaseList {
			databases[i] = &api.DatabaseInfo{
				Db: l,
			}
		}
		return &Response{
			Response: &api.ListDatabasesResponse{
				Databases: databases,
			},
		}, ctx, nil
	} else if runner.describe != nil {
		db, err := runner.GetDatabase(ctx, tx, tenant, runner.describe.GetDb())
		if err != nil {
			return nil, ctx, err
		}

		collectionList := db.ListCollection()

		var collections = make([]*api.CollectionDescription, len(collectionList))
		for i, c := range collectionList {
			collections[i] = &api.CollectionDescription{
				Collection: c.GetName(),
				Metadata:   &api.CollectionMetadata{},
				Schema:     c.Schema,
			}
		}

		return &Response{
			Response: &api.DescribeDatabaseResponse{
				Db:          db.Name(),
				Metadata:    &api.DatabaseMetadata{},
				Collections: collections,
			},
		}, ctx, nil
	}

	return &Response{}, ctx, api.Errorf(codes.Unknown, "unknown request path")
}
