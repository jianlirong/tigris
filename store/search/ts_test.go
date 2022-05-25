package search

import (
	"encoding/json"
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/buger/jsonparser"
	"testing"
)

func TestTs(t *testing.T) {
	var list = []interface{}{1, "foo", []byte{0x01}, []byte("foo"), []byte(`1`), []byte(`foo`)}
	for _, l := range list {
		switch ty := l.(type) {
		case []byte:
			doc := []byte(`{"a": 1, "b":"foo"}`)
			doc, _ = jsonparser.Set(doc, ty, "id")
			fmt.Println(string(doc))
		default:
			b, _ := json.Marshal(ty)
			fmt.Println(string(b))
			doc := []byte(`{"a": 1, "b":"foo"}`)
			doc, _ = jsonparser.Set(doc, b, "id")
			fmt.Println(string(doc))
		}
	}

	var tp = tuple.Tuple{"foo", 10}
	fmt.Println(string(tp.Pack()))
	b, _ := json.Marshal(tp.Pack())
	fmt.Println(string(b))
}
