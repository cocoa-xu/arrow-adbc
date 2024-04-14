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

package mocks

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
)

type statement struct {
	Query         string
	StringOption1 string
	StringOption2 string
	StringOption3 string
	BytesOption1  []byte
	BytesOption2  []byte
	BytesOption3  []byte
	IntOption1    int64
	IntOption2    int64
	IntOption3    int64
	DoubleOption1 float64
	DoubleOption2 float64
	DoubleOption3 float64

	ExtraStringOptions map[string]string
	ExtraBytesOptions  map[string][]byte
	ExtraIntOptions    map[string]int64
	ExtraDoubleOptions map[string]float64
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() error {
	return nil
}

func (st *statement) GetOption(key string) (string, error) {
	switch key {
	case StatementOptionStringOption1:
		return st.StringOption1, nil
	case StatementOptionStringOption2:
		return st.StringOption2, nil
	case StatementOptionStringOption3:
		return st.StringOption3, nil
	default:
		val, ok := st.ExtraStringOptions[key]
		if ok {
			return val, nil
		}
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement option `%s`", key),
		}
	}
}

func (st *statement) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case StatementOptionBytesOption1:
		return st.BytesOption1, nil
	case StatementOptionBytesOption2:
		return st.BytesOption2, nil
	case StatementOptionBytesOption3:
		return st.BytesOption3, nil
	default:
		val, ok := st.ExtraBytesOptions[key]
		if ok {
			return val, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement option `%s`", key),
		}
	}
}

func (st *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case StatementOptionIntOption1:
		return st.IntOption1, nil
	case StatementOptionIntOption2:
		return st.IntOption2, nil
	case StatementOptionIntOption3:
		return st.IntOption3, nil
	default:
		val, ok := st.ExtraIntOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement option `%s`", key),
		}
	}
}

func (st *statement) GetOptionDouble(key string) (float64, error) {
	switch key {
	case StatementOptionDoubleOption1:
		return st.DoubleOption1, nil
	case StatementOptionDoubleOption2:
		return st.DoubleOption2, nil
	case StatementOptionDoubleOption3:
		return st.DoubleOption3, nil
	default:
		val, ok := st.ExtraDoubleOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement option `%s`", key),
		}
	}
}

func (st *statement) SetOptions(options map[string]string) error {
	for k, v := range options {
		v := v // copy into loop scope
		switch k {
		case StatementOptionStringOption1:
			st.StringOption1 = v
		case StatementOptionStringOption2:
			st.StringOption2 = v
		case StatementOptionStringOption3:
			st.StringOption3 = v
		default:
			err := st.checkExpectedError(k)
			if err != nil {
				return err
			}
			st.ExtraStringOptions[k] = v
		}
	}
	return nil
}

func (st *statement) SetOption(key string, value string) error {
	switch key {
	case StatementOptionStringOption1:
		st.StringOption1 = value
	case StatementOptionStringOption2:
		st.StringOption2 = value
	case StatementOptionStringOption3:
		st.StringOption3 = value
	default:
		err := st.checkExpectedError(key)
		if err != nil {
			return err
		}
		st.ExtraStringOptions[key] = value
	}
	return nil
}

func (st *statement) SetOptionBytes(key string, value []byte) error {
	switch key {
	case StatementOptionBytesOption1:
		st.BytesOption1 = value
	case StatementOptionBytesOption2:
		st.BytesOption2 = value
	case StatementOptionBytesOption3:
		st.BytesOption3 = value
	default:
		err := st.checkExpectedError(key)
		if err != nil {
			return err
		}
		st.ExtraBytesOptions[key] = value
	}
	return nil
}

func (st *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case StatementOptionIntOption1:
		st.IntOption1 = value
	case StatementOptionIntOption2:
		st.IntOption2 = value
	case StatementOptionIntOption3:
		st.IntOption3 = value
	default:
		err := st.checkExpectedError(key)
		if err != nil {
			return err
		}
		st.ExtraIntOptions[key] = value
	}
	return nil
}

func (st *statement) SetOptionDouble(key string, value float64) error {
	switch key {
	case StatementOptionDoubleOption1:
		st.DoubleOption1 = value
	case StatementOptionDoubleOption2:
		st.DoubleOption2 = value
	case StatementOptionDoubleOption3:
		st.DoubleOption3 = value
	default:
		err := st.checkExpectedError(key)
		if err != nil {
			return err
		}
		st.ExtraDoubleOptions[key] = value
	}
	return nil
}

func (st *statement) checkExpectedError(key string) error {
	if strings.Contains(key, "expected-error") {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement option `%s`", key),
		}
	}
	return nil
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (st *statement) SetSqlQuery(query string) error {
	st.Query = query
	return nil
}

// ExecuteQuery executes the current query or prepared statement
// and returnes a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	return nil, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteQuery not yet implemented for mocks driver",
	}
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for mocks driver",
	}
}

// ExecuteSchema gets the schema of the result set of a query without executing it.
func (st *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteSchema not yet implemented for mocks driver",
	}
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (st *statement) Prepare(_ context.Context) error {
	return nil
}

// SetSubstraitPlan allows setting a serialized Substrait execution
// plan into the query or for querying Substrait-related metadata.
//
// Drivers are not required to support both SQL and Substrait semantics.
// If they do, it may be via converting between representations internally.
//
// Like SetSqlQuery, after this is called the query can be executed
// using any of the Execute methods. If the query is expected to be
// executed repeatedly, Prepare should be called first on the statement.
func (st *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait not yet implemented for mocks driver",
	}
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (st *statement) Bind(_ context.Context, values arrow.Record) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Bind not yet implemented for mocks driver",
	}
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (st *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "BindStream not yet implemented for mocks driver",
	}
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
//
// This retrieves an Arrow Schema describing the number, names, and
// types of the parameters in a parameterized statement. The fields
// of the schema should be in order of the ordinal position of the
// parameters; named parameters should appear only once.
//
// If the parameter does not have a name, or a name cannot be determined,
// the name of the corresponding field in the schema will be an empty
// string. If the type cannot be determined, the type of the corresponding
// field will be NA (NullType).
//
// This should be called only after calling Prepare.
//
// This should return an error with StatusNotImplemented if the schema
// cannot be determined.
func (st *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetParameterSchema not yet implemented for mocks driver",
	}
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
//
// It returns the Schema of the result set, the collection of partition
// descriptors and the number of rows affected, if known. If unknown,
// the number of rows affected will be -1.
//
// If the driver does not support partitioned results, this will return
// an error with a StatusNotImplemented code.
func (st *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecutePartitions not yet implemented for mocks driver",
	}
}
