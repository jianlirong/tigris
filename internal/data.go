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

package internal

import (
	"bytes"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	ulog "github.com/tigrisdata/tigris/util/log"
	"github.com/ugorji/go/codec"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	bh codec.BincHandle
)

// DataType is to define the different data types for the data stored in the storage engine.
type DataType byte

// Note: Do not change the order. Order is important because encoder is adding the type as the first byte. Check the
// Encode/Decode method to see how it is getting used.
const (
	Unknown DataType = iota
	TableDataType
)

const (
	JsonEncoding = iota + 1
)

func NewTimestamp() *Timestamp {
	ts := time.Now().UTC()
	return &Timestamp{
		Seconds:     ts.Unix(),
		Nanoseconds: int64(ts.Nanosecond()),
	}
}

func (ts *Timestamp) ToRFC3339() string {
	gotime := time.Unix(ts.Seconds, ts.Nanoseconds).UTC()
	return gotime.Format(time.RFC3339)
}

func (ts *Timestamp) GetProtoTS() *timestamppb.Timestamp {
	return &timestamppb.Timestamp{
		Seconds: ts.Seconds,
		Nanos:   int32(ts.Nanoseconds),
	}
}

// NewTableData returns a table data type by setting the ts to the current value.
func NewTableData(data []byte) *TableData {
	return &TableData{
		CreatedAt: NewTimestamp(),
		RawData:   data,
	}
}

func NewTableDataWithTS(createdAt *Timestamp, updatedAt *Timestamp, data []byte) *TableData {
	return &TableData{
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		RawData:   data,
	}
}

func NewTableDataWithEncoding(data []byte, encoding int32) *TableData {
	return &TableData{
		CreatedAt: NewTimestamp(),
		RawData:   data,
		Encoding:  encoding,
	}
}

// Encode is used to encode data to the raw bytes which is used to store in storage as value. The first byte is storing
// the type corresponding to this Data. This is important and used by the decoder later to decode back.
func Encode(data *TableData) ([]byte, error) {
	var buf bytes.Buffer
	// this is added so that we can evolve the DataTypes and have more dataTypes in future
	err := buf.WriteByte(byte(TableDataType))
	if err != nil {
		return nil, err
	}
	enc := codec.NewEncoder(&buf, &bh)
	if err := enc.Encode(data); ulog.E(err) {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode is used to decode the raw bytes to TableData. The raw bytes are returned from the storage and the kvStore is
// calling Decode to convert these raw bytes back to TableData.
func Decode(b []byte) (*TableData, error) {
	dataType := DataType(b[0])
	return decodeInternal(dataType, b[1:])
}

func decodeInternal(dataType DataType, encoded []byte) (*TableData, error) {
	dec := codec.NewDecoderBytes(encoded, &bh)

	switch dataType {
	case TableDataType:
		var v *TableData
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		return v, nil
	}

	return nil, api.Errorf(api.Code_INTERNAL, "unable to decode '%v'", dataType)
}
