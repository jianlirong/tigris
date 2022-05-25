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
	"context"
	"fmt"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
	"io"
)

type Store interface {
	CreateCollection(ctx context.Context, schema *tsApi.CollectionSchema) error
	DropCollection(ctx context.Context, table string) error
	IndexDocuments(_ context.Context, table string, documents io.Reader, options IndexDocumentsOptions) error
	Search(_ context.Context, table string, filterBy string) ([]tsApi.SearchResult, error)
}

func NewStore(config *config.SearchConfig) (Store, error) {
	client := typesense.NewClient(
		typesense.WithServer(fmt.Sprintf("http://%s:%d", config.Host, config.Port)),
		typesense.WithAPIKey(config.AuthKey))
	return &storeImpl{
		client: client,
	}, nil
}
