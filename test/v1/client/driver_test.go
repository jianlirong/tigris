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

//go:build integration

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	api "github.com/tigrisdata/tigris-client-go/api/server/v1"
	clientConfig "github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
	"github.com/tigrisdata/tigris/server/config"
	"google.golang.org/grpc/codes"
)

func getTestServerHostPort() (string, int16) {
	config.LoadEnvironment() // Move this to test.Main

	if config.GetEnvironment() == config.EnvTest {
		return "tigris_server", 8081
	}
	return "localhost", 8081
}

func getDocuments(t *testing.T, db driver.Database, filter driver.Filter) []driver.Document {
	ctx := context.Background()

	it, err := db.Read(ctx, "c1", filter, nil, &driver.ReadOptions{})
	require.NoError(t, err)

	var documents []driver.Document
	var doc driver.Document
	for it.Next(&doc) {
		documents = append(documents, doc)
	}
	return documents
}

func testRead(t *testing.T, db driver.Database, filter driver.Filter, expected []driver.Document) {
	ctx := context.Background()

	it, err := db.Read(ctx, "c1", filter, nil)
	require.NoError(t, err)

	var doc driver.Document
	var i int
	for it.Next(&doc) {
		require.Greater(t, len(expected), i)
		assert.JSONEq(t, string(expected[i]), string(doc))
		i++
	}

	require.Equal(t, len(expected), i)
	assert.NoError(t, it.Err())
}

func testTxReadWrite(t *testing.T, c driver.Driver) {
	ctx := context.TODO()

	dbName := "db_client_test"
	_ = c.DropDatabase(ctx, dbName)

	schema := `{
		"title": "c1",
		"properties": {
			"str_field": {
				"type": "string"
			},
			"int_field": {
				"type": "integer"
			},
			"bool_field": {
				"type": "boolean"
			}
		},
		"primary_key": ["str_field"]
	}`

	err := c.CreateDatabase(ctx, dbName)
	require.NoError(t, err)
	db1 := c.UseDatabase(dbName)
	err = db1.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
	require.NoError(t, err)

	doc1 := driver.Document(`{"str_field": "value1", "int_field": 111, "bool_field": true}`)
	doc2 := driver.Document(`{"str_field": "value2", "int_field": 222, "bool_field": false}`)
	resp, err := db1.Insert(ctx, "c1", []driver.Document{
		doc1,
		doc2,
	})
	require.NoError(t, err)
	require.Equal(t, "inserted", resp.Status)

	testRead(t, db1, driver.Filter(`{"str_field": "value2"}`), []driver.Document{doc2})

	delResp, err := db1.Delete(ctx, "c1", driver.Filter(`{"str_field": "value2"}`))
	require.NoError(t, err)
	require.Equal(t, "deleted", delResp.Status)

	testRead(t, db1, driver.Filter(`{"str_field": "value2"}`), nil)

	for {
		tx, err := c.BeginTx(ctx, dbName)
		require.NoError(t, err)

		doc3 := driver.Document(`{"str_field": "value3", "int_field": 333, "bool_field": false}`)
		resp, err = tx.Insert(ctx, "c1", []driver.Document{
			doc2,
			doc3,
		})
		require.NoError(t, err)
		require.Equal(t, "inserted", resp.Status)

		it, err := tx.Read(ctx, "c1", driver.Filter(`{"str_field": "value2"}`), nil)
		require.NoError(t, err)
		var doc driver.Document
		for it.Next(&doc) {
			assert.JSONEq(t, string(doc2), string(doc))
		}
		if err = tx.Commit(ctx); err == nil || err.Error() != "transaction not committed due to conflict with another transaction" {
			break
		}
	}
	require.NoError(t, err)
}

func testDriverBinary(t *testing.T, c driver.Driver) {
	ctx := context.TODO()

	dbName := "db_client_test"
	_ = c.DropDatabase(ctx, dbName, &driver.DatabaseOptions{})

	db1 := c.UseDatabase(dbName)
	_ = db1.DropCollection(ctx, "c1", &driver.CollectionOptions{})

	schema := `{
		"title": "c1",
		"properties": {
			"K1": {
				"type": "string",
				"format": "byte"
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1"]
	}`

	err := c.CreateDatabase(ctx, dbName)
	require.NoError(t, err, " dbName %s", dbName)

	db1 = c.UseDatabase(dbName)
	err = db1.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
	require.NoError(t, err)

	err = db1.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
	require.Error(t, api.Errorf(codes.AlreadyExists, "collection already exist"), err)

	type doc struct {
		K1 []byte
		D1 string
	}

	doc1 := doc{
		K1: []byte("vK1"),
		D1: "vD1",
	}
	docEnc, err := json.Marshal(doc1)
	require.NoError(t, err)

	_, err = db1.Insert(ctx, "c1", []driver.Document{docEnc})
	require.NoError(t, err)

	doc2 := doc{
		K1: []byte(`1234`),
		D1: "vD2",
	}
	docEnc, err = json.Marshal(doc2)
	require.NoError(t, err)

	_, err = db1.Insert(ctx, "c1", []driver.Document{docEnc})
	require.NoError(t, err)

	filterEnc, err := json.Marshal(map[string]interface{}{
		"K1": []byte("vK1"),
	})
	require.NoError(t, err)

	var actualDoc doc
	docs := getDocuments(t, db1, filterEnc)
	require.Greater(t, len(docs), 0)
	require.NoError(t, json.Unmarshal(docs[0], &actualDoc))
	require.Equal(t, doc1, actualDoc)

	filterEnc, err = json.Marshal(map[string]interface{}{
		"K1": []byte(`1234`),
	})
	require.NoError(t, err)

	docs = getDocuments(t, db1, filterEnc)
	require.Greater(t, len(docs), 0)
	require.NoError(t, json.Unmarshal(docs[0], &actualDoc))
	require.Equal(t, doc2, actualDoc)

	err = db1.DropCollection(ctx, "c1")
	require.NoError(t, err)
}

func testDriver(t *testing.T, c driver.Driver) {
	ctx := context.TODO()

	dbName := "db_client_test"
	_ = c.DropDatabase(ctx, dbName)

	schema := `{
		"title": "c1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`

	err := c.CreateDatabase(ctx, dbName)
	require.NoError(t, err)
	defer func() {
		_ = c.DropDatabase(ctx, dbName)
	}()

	db1 := c.UseDatabase(dbName)
	err = db1.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
	require.NoError(t, err)

	err = db1.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
	require.Error(t, api.Errorf(codes.AlreadyExists, "collection already exist"), err)

	doc1 := driver.Document(`{"K1": "vK1", "K2": 1, "D1": "vD1"}`)

	_, err = db1.Insert(ctx, "c1", []driver.Document{doc1})
	require.NoError(t, err)

	_, err = db1.Insert(ctx, "c1", []driver.Document{doc1})
	require.Error(t, api.Errorf(codes.AlreadyExists, "row already exists"), err)

	doc2, doc3 := driver.Document(`{"K1": "vK1", "K2": 2, "D1": "vD2"}`), driver.Document(`{"K1": "vK1", "K2": 3, "D1": "vD3"}`)

	// multiple docs
	_, err = db1.Insert(ctx, "c1", []driver.Document{doc2, doc3})
	require.NoError(t, err)

	fl := driver.Filter(`{ "$or" : [ {"$and" : [ {"K1" : "vK1"}, {"K2" : 1} ]}, {"$and" : [ {"K1" : "vK1"}, {"K2" : 3} ]} ]}`)
	testRead(t, db1, fl, []driver.Document{doc1, doc3})

	flupd := driver.Filter(`{"$and" : [ {"K1" : "vK1"}, {"K2" : 2} ]}`)
	_, err = db1.Update(ctx, "c1", flupd, driver.Update(`{"$set":{"D1": "1000"}}`))
	require.NoError(t, err)

	_, err = db1.Delete(ctx, "c1", fl)
	require.NoError(t, err)

	doc4 := driver.Document(`{"K1": "vK1", "K2": 2, "D1": "1000"}`)
	testRead(t, db1, driver.Filter("{}"), []driver.Document{doc4})

	err = db1.DropCollection(ctx, "c1")
	require.NoError(t, err)
}

func testTxClient(t *testing.T, c driver.Driver) {
	ctx := context.TODO()

	dbName := "db_client_test"
	_ = c.DropDatabase(ctx, dbName)

	schema := `{
		"title": "c1",
		"description": "this schema is for client integration tests",
		"properties": {
			"K1": {
				"type": "string"
			},
			"K2": {
				"type": "integer"
			},
			"D1": {
				"type": "string",
				"maxLength": 128
			}
		},
		"primary_key": ["K1", "K2"]
	}`

	err := c.CreateDatabase(ctx, dbName)
	require.NoError(t, err)
	defer func() {
		//_ = c.DropDatabase(ctx, dbName)
	}()

	db1 := c.UseDatabase(dbName)

	doc1 := driver.Document(`{"K1": "vK1", "K2": 1, "D1": "vD1"}`)
	doc2, doc3 := driver.Document(`{"K1": "vK1", "K2": 2, "D1": "vD2"}`), driver.Document(`{"K1": "vK1", "K2": 3, "D1": "vD3"}`)
	for {
		tx, err := c.BeginTx(ctx, dbName)
		err = tx.CreateOrUpdateCollection(ctx, "c1", driver.Schema(schema))
		require.NoError(t, err)

		_, err = tx.Insert(ctx, "c1", []driver.Document{doc1})
		require.NoError(t, err)

		// multiple docs
		_, err = tx.Insert(ctx, "c1", []driver.Document{doc2, doc3})
		require.NoError(t, err)

		if err = tx.Commit(ctx); err != nil && err.Error() == "transaction not committed due to conflict with another transaction" {
			continue
		}
		require.NoError(t, err)
		break
	}

	fl := driver.Filter(`{ "$or" : [ {"$and" : [ {"K1" : "vK1"}, {"K2" : 1} ]}, {"$and" : [ {"K1" : "vK1"}, {"K2" : 3} ]} ]}`)
	testRead(t, db1, fl, []driver.Document{doc1, doc3})

	_, err = db1.Delete(ctx, "c1", fl)
	require.NoError(t, err)

	testRead(t, db1, driver.Filter("{}"), []driver.Document{doc2})

	_, err = db1.Delete(ctx, "c1", driver.Filter(`{"K1" : "vK1", "K2" : 2}`))
	require.NoError(t, err)

	testRead(t, db1, driver.Filter("{}"), nil)

	tx, err := c.BeginTx(ctx, dbName, &driver.TxOptions{})

	_, err = tx.Insert(ctx, "c1", []driver.Document{doc1})
	require.NoError(t, err)

	//multiple documents
	_, err = tx.Insert(ctx, "c1", []driver.Document{doc2, doc3})
	require.NoError(t, err)

	err = tx.Rollback(ctx)
	require.NoError(t, err)

	testRead(t, db1, driver.Filter("{}"), nil)

	err = db1.DropCollection(ctx, "c1")
	require.NoError(t, err)
}

func TestDriverGRPCC(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.GRPC
	c, err := driver.NewDriver(context.Background(), &clientConfig.Driver{
		URL: fmt.Sprintf("%s:%d", h, p),
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testDriver(t, c)
	testDriverBinary(t, c)
}

func TestDriverHTTP(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.HTTP
	c, err := driver.NewDriver(context.Background(), &clientConfig.Driver{
		URL: fmt.Sprintf("http://%s:%d", h, p),
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testDriver(t, c)
	testDriverBinary(t, c)
}

func TestDriverTxGRPC(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.GRPC
	c, err := driver.NewDriver(context.Background(), &clientConfig.Driver{
		URL: fmt.Sprintf("%s:%d", h, p),
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testTxClient(t, c)
	testTxReadWrite(t, c)
}

func TestDriverTxHTTPDriver(t *testing.T) {
	h, p := getTestServerHostPort()
	driver.DefaultProtocol = driver.HTTP
	c, err := driver.NewDriver(context.Background(), &clientConfig.Driver{
		URL: fmt.Sprintf("http://%s:%d", h, p),
	})
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	testTxClient(t, c)
	testTxReadWrite(t, c)
}
