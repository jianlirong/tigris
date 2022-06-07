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
	"context"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
)

type TxListener interface {
	OnCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error
	OnPostCommit(context.Context, *metadata.Tenant, kv.EventListener) error
	OnRollback(context.Context, *metadata.Tenant, kv.EventListener)
}

type NoopTxListener struct{}

func (l *NoopTxListener) OnCommit(context.Context, *metadata.Tenant, transaction.Tx, kv.EventListener) error {
	return nil
}
func (l *NoopTxListener) OnPostCommit(context.Context, *metadata.Tenant, kv.EventListener) error {
	return nil
}
func (l *NoopTxListener) OnRollback(context.Context, *metadata.Tenant, kv.EventListener) {}
