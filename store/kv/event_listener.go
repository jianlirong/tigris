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

package kv

import (
	"context"
)

const (
	InsertEvent      = "insert"
	ReplaceEvent     = "replace"
	UpdateEvent      = "update"
	UpdateRangeEvent = "updateRange"
	DeleteEvent      = "delete"
	DeleteRangeEvent = "deleteRange"
)

type EventListenerCtxKey struct{}

type EventListener interface {
	OnSet(op string, table []byte, key []byte, data []byte)
	OnClearRange(op string, table []byte, lKey []byte, rKey []byte)
	GetEvents() []*Event
}

type Event struct {
	Op    string
	Table []byte
	Key   []byte `json:",omitempty"`
	LKey  []byte `json:",omitempty"`
	RKey  []byte `json:",omitempty"`
	Data  []byte `json:",omitempty"`
	Last  bool
}

type DefaultListener struct {
	Events []*Event
}

func (l *DefaultListener) OnSet(op string, table []byte, key []byte, data []byte) {
	l.Events = append(l.Events, &Event{
		Op:    op,
		Table: table,
		Key:   key,
		Data:  data,
	})
}
func (l *DefaultListener) OnClearRange(op string, table []byte, lKey []byte, rKey []byte) {
	l.Events = append(l.Events, &Event{
		Op:    op,
		Table: table,
		Key:   lKey,
		LKey:  lKey,
		RKey:  rKey,
	})
}
func (l *DefaultListener) GetEvents() []*Event {
	return l.Events
}

type NoopEventListener struct{}

func (l *NoopEventListener) OnSet(op string, table []byte, key []byte, data []byte)         {}
func (l *NoopEventListener) OnClearRange(op string, table []byte, lKey []byte, rKey []byte) {}
func (l *NoopEventListener) GetEvents() []*Event                                            { return nil }

func WrapEventListenerCtx(ctx context.Context) context.Context {
	return context.WithValue(ctx, EventListenerCtxKey{}, &DefaultListener{})
}

func GetEventListener(ctx context.Context) EventListener {
	a := ctx.Value(EventListenerCtxKey{})
	if a != nil {
		if conv, ok := a.(EventListener); ok {
			return conv
		}
	}

	return &NoopEventListener{}
}
