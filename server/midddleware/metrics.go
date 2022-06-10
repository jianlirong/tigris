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

package middleware

import (
	"context"
	"github.com/tigrisdata/tigris/server/metrics"
	"github.com/uber-go/tally"
	"google.golang.org/grpc"
	"strings"
)

type grpcCallData struct {
	grpcMethod string
	grpcType   string
}

func newGrpcCallData(grpcMethod string, grpcType string) grpcCallData {
	return grpcCallData{grpcMethod: grpcMethod, grpcType: grpcType}
}

func (g *grpcCallData) getTags() map[string]string {
	fullMethodParts := strings.Split(g.grpcMethod, "/")
	grpcService := fullMethodParts[1]
	grpcMethod := fullMethodParts[2]
	return map[string]string{
		"grpc_method":  grpcMethod,
		"grpc_service": grpcService,
		"grpc_type":    g.grpcType,
	}
}

func (g *grpcCallData) getGrpcCounter(name string) tally.Counter {
	tags := g.getTags()
	return metrics.Root.Tagged(tags).Counter(name)
}

func (g *grpcCallData) increaseGrpcCounter(counter tally.Counter, value int64) {
	counter.Inc(value)
}

func (g *grpcCallData) receiveMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_received_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcCallData) handleMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_handled_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcCallData) errorMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_error_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcCallData) okMessage() {
	counter := g.getGrpcCounter("grpc_server_msg_ok_total")
	g.increaseGrpcCounter(counter, 1)
}

func (g *grpcCallData) getTimeHistogram() tally.Histogram {
	tags := g.getTags()
	return metrics.Root.Tagged(tags).Histogram("grpc_server_handling_time_bucket", tally.DefaultBuckets)
}

func UnaryMetricsServerInterceptor() func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		callData := newGrpcCallData(info.FullMethod, "unary")
		timeHistogram := callData.getTimeHistogram()
		stopWatch := timeHistogram.Start()
		callData.receiveMessage()
		resp, err := handler(ctx, req)
		callData.handleMessage()
		if err != nil {
			callData.errorMessage()
		} else {
			callData.okMessage()
		}
		stopWatch.Stop()
		return resp, err
	}
}
func StreamMetricsServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		callData := newGrpcCallData(info.FullMethod, "stream")
		timeHistogram := callData.getTimeHistogram()
		stopWatch := timeHistogram.Start()
		callData.receiveMessage()
		wrapper := &recvWrapper{stream}
		callData.handleMessage()
		err := handler(srv, wrapper)
		if err != nil {
			callData.okMessage()
		} else {
			callData.errorMessage()
		}
		stopWatch.Stop()
		return err
	}
}
