package search

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/server/config"
	"testing"
)

func TestIndexingDocuments(t *testing.T) {
	s, err := NewStore(&config.DefaultConfig.Search)
	require.NoError(t, err)

	reader := bytes.NewReader([]byte(`{"name": 1, "id": "200"}`))
	fmt.Println(s.IndexDocuments(nil, "default_namespace-db1-catalog1", reader, IndexDocumentsOptions{
		Action:    "create",
		BatchSize: 1,
	}))
}
