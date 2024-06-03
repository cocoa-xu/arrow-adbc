// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/ch-go/proto"
	"io"
	"strings"
	"sync/atomic"

	"github.com/ClickHouse/ch-go"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.Record
	curChIndex int
	rec        arrow.Record
	err        error

	cancelFn context.CancelFunc
}

func checkContext(ctx context.Context, maybeErr error) error {
	if maybeErr != nil {
		return maybeErr
	} else if ctx.Err() == context.Canceled {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if ctx.Err() == context.DeadlineExceeded {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

func buildFloat64Array(col proto.ColResult, nullable bool, rows int64, builder *array.Float64Builder) {
	if nullable {
		c := col.(*proto.ColNullable[float64])
		for i := 0; i < int(rows); i++ {
			v := c.Row(i)
			if v.Set {
				builder.Append(v.Value)
			} else {
				builder.AppendNull()
			}
		}
	} else {
		c := col.(*proto.ColFloat64)
		for i := 0; i < int(rows); i++ {
			builder.Append(c.Row(i))
		}
	}
}

func buildStringArray(col proto.ColResult, nullable bool, rows int64, builder *array.StringBuilder) {
	if nullable {
		c := col.(*proto.ColNullable[string])
		for i := 0; i < int(rows); i++ {
			v := c.Row(i)
			if v.Set {
				builder.Append(v.Value)
			} else {
				builder.AppendNull()
			}
		}
	} else {
		c := col.(*proto.ColStr)
		for i := 0; i < int(rows); i++ {
			builder.Append(c.Row(i))
		}
	}
}

func sendBatch(schema *arrow.Schema, fieldBuilders []array.Builder, numRows int, ch chan arrow.Record) {
	fieldValues := make([]arrow.Array, len(fieldBuilders))
	for i := range fieldBuilders {
		fieldValues[i] = fieldBuilders[i].NewArray()
	}
	rec := array.NewRecord(schema, fieldValues, int64(numRows))
	ch <- rec
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, parameters array.RecordReader, client *ch.Client, query string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (rdr *reader, totalRows int64, err error) {
	var results proto.Results
	if err := client.Do(ctx, ch.Query{
		Body:   query,
		Result: results.Auto(),
	}); err != nil {
		return nil, 0, err
	}
	totalRows = int64(results.Rows())

	// todo: handle bulk queries and query parameters
	numChannels := 1
	chs := make([]chan arrow.Record, numChannels)
	ch0 := make(chan arrow.Record, resultRecordBufferSize)
	chs[0] = ch0
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefetchConcurrency)
	ctx, cancelFn := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			close(ch0)
			cancelFn()
		}
	}()

	rdr = &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	fields := make([]arrow.Field, 0)
	for _, result := range results {
		columnType := result.Data.Type()
		typeLen := len(columnType)
		// todo: `buildSchemaField` can be improved if we choose to use `ch-go`
		field, err := buildSchemaField(result.Name, columnType.String(), 0, typeLen)
		if err != nil {
			return nil, 0, err
		}
		fields = append(fields, field)
	}
	schema := arrow.NewSchema(fields, nil)
	rdr.schema = schema

	numColumns := len(fields)
	group.Go(func() error {
		schemaFields := schema.Fields()
		fieldBuilders := make([]array.Builder, numColumns)
		for i, schemaField := range schemaFields {
			fieldBuilders[i] = array.NewBuilder(alloc, schemaField.Type)
			result := results[i]
			nullable := schemaField.Nullable
			switch schemaField.Type.ID() {
			case arrow.FLOAT64:
				buildFloat64Array(result.Data, nullable, totalRows, fieldBuilders[i].(*array.Float64Builder))
			case arrow.STRING:
				buildStringArray(result.Data, nullable, totalRows, fieldBuilders[i].(*array.StringBuilder))
			default:
				return adbc.Error{
					Code: adbc.StatusNotImplemented,
					Msg:  fmt.Sprintf("type: %v not supported yet", schemaField.Type),
				}
			}
		}
		sendBatch(schema, fieldBuilders, int(totalRows), ch0)
		return checkContext(ctx, rdr.Err())
	})

	lastChannelIndex := len(chs) - 1
	go func() {
		// place this here so that we always clean up, but they can't be in a
		// separate goroutine. Otherwise we'll have a race condition between
		// the call to wait and the calls to group.Go to kick off the jobs
		// to perform the pre-fetching (GH-1283).
		rdr.err = group.Wait()
		// don't close the last channel until after the group is finished,
		// so that Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
	}()

	return rdr, totalRows, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}
	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.Record {
	return r.rec
}

func parseType(typeString string, typeIndex, typeLen int) (string, int, error) {
	typeEnd := -2
	for i := typeIndex; i < typeLen; i++ {
		c := typeString[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			typeEnd = i
		} else {
			break
		}
	}
	typeEnd += 1
	if typeEnd == -1 {
		return "", 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("unknown type `%s`", typeString),
		}
	}

	return typeString[typeIndex:typeEnd], typeEnd, nil
}

func expectRightBracket(typeString string, typeIndex, typeLen int) (int, error) {
	if typeIndex == typeLen {
		return 0, io.EOF
	}

	for i := typeIndex; i < typeLen; i++ {
		if typeString[i] == ')' {
			return i + 1, nil
		} else if typeString[i] == ' ' || typeString[i] == '\t' || typeString[i] == '\n' {
			continue
		} else {
			return 0, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("expecting a `)` but got: `%c` at %d", typeString[i], i),
			}
		}
	}
	return typeLen, io.EOF
}

var (
	simpleDataType = map[string]arrow.DataType{
		"Int8":    arrow.PrimitiveTypes.Int8,
		"UInt8":   arrow.PrimitiveTypes.Uint8,
		"Int16":   arrow.PrimitiveTypes.Int16,
		"UInt16":  arrow.PrimitiveTypes.Uint16,
		"Int32":   arrow.PrimitiveTypes.Int32,
		"UInt32":  arrow.PrimitiveTypes.Uint32,
		"Int64":   arrow.PrimitiveTypes.Int64,
		"UInt64":  arrow.PrimitiveTypes.Uint64,
		"Float32": arrow.PrimitiveTypes.Float32,
		"Float64": arrow.PrimitiveTypes.Float64,
		"String":  arrow.BinaryTypes.String,
	}
)

func buildField(name, typeName string) (arrow.Field, error) {
	dataType, ok := simpleDataType[typeName]
	if !ok {
		return arrow.Field{}, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unsupported type: `%s`", typeName),
		}
	}

	field := arrow.Field{
		Name:     name,
		Type:     dataType,
		Nullable: false,
	}
	return field, nil
}

func ensureConsumedTypeString(typeString string, typeIndex, typeLen int) error {
	if typeIndex == typeLen {
		return nil
	}
	for i := typeIndex; i < typeLen; i++ {
		if typeString[i] == ' ' || typeString[i] == '\t' || typeString[i] == '\n' {
			continue
		} else {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("cannot fully consume the type string `%s`, parsed up to index %d", typeString, i),
			}
		}
	}
	return nil
}

func buildSchemaField(name string, typeString string, typeIndex, typeLen int) (arrow.Field, error) {
	if strings.HasPrefix(typeString, "Nullable(") {
		typeName, typeIndex, err := parseType(typeString, typeIndex+9, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		typeIndex, err = expectRightBracket(typeString, typeIndex, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		if err = ensureConsumedTypeString(typeString, typeIndex, typeLen); err != nil {
			return arrow.Field{}, err
		}

		field, err := buildField(name, typeName)
		field.Nullable = true
		return field, err
	} else {
		typeName, typeIndex, err := parseType(typeString, typeIndex, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		if err = ensureConsumedTypeString(typeString, typeIndex, typeLen); err != nil {
			return arrow.Field{}, err
		}
		return buildField(name, typeName)
	}
}
