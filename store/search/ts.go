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
	jsoniter "github.com/json-iterator/go"
	"github.com/typesense/typesense-go/typesense"
	tsApi "github.com/typesense/typesense-go/typesense/api"
	"io"
	"net/http"
)

type storeImpl struct {
	client *typesense.Client
}

const (
	searchID = "id"
)

type IndexDocumentsOptions struct {
	Action    string
	BatchSize int
}

func (s *storeImpl) DeleteDocuments(_ context.Context, table string, key string) error {
	_, err := s.client.Collection(table).Document(key).Delete()
	return err
}

func (s *storeImpl) IndexDocuments(_ context.Context, table string, documents io.Reader, options IndexDocumentsOptions) (err error) {
	var closer io.ReadCloser
	closer, err = s.client.Collection(table).Documents().ImportJsonl(documents, &tsApi.ImportDocumentsParams{
		Action:    &options.Action,
		BatchSize: &options.BatchSize,
	})
	defer func() {
		type resp struct {
			Code     int
			Document string
			Error    string
			Success  bool
		}
		if closer != nil {
			var r resp
			res, _ := io.ReadAll(closer)
			_ = jsoniter.Unmarshal(res, &r)
			if len(r.Error) > 0 {
				err = fmt.Errorf(r.Error)
			}
			closer.Close()
		}
	}()
	return err
}

func (s *storeImpl) Search(_ context.Context, table string, filterBy string) ([]tsApi.SearchResult, error) {
	q := "*"
	res, err := s.client.MultiSearch.Perform(&tsApi.MultiSearchParams{}, tsApi.MultiSearchSearchesParameter{
		Searches: []tsApi.MultiSearchCollectionParameters{
			{
				Collection: table,
				MultiSearchParameters: tsApi.MultiSearchParameters{
					FilterBy: &filterBy,
					Q:        &q,
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	return res.Results, nil
}

func (s *storeImpl) CreateCollection(_ context.Context, schema *tsApi.CollectionSchema) error {
	_, err := s.client.Collections().Create(schema)
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case *typesense.HTTPError:
		if e.Status == http.StatusConflict {
			return ErrDuplicateEntity
		}
		return NewSearchError(e.Status, ErrCodeUnhandled, e.Error())
	}

	return NewSearchError(0, ErrCodeUnhandled, err.Error())
}

func (s *storeImpl) DropCollection(_ context.Context, table string) error {
	_, err := s.client.Collection(table).Delete()
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case *typesense.HTTPError:
		if e.Status == http.StatusNotFound {
			return ErrNotFound
		}
		return NewSearchError(e.Status, ErrCodeUnhandled, e.Error())
	}

	return NewSearchError(0, ErrCodeUnhandled, err.Error())
}
