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
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"strings"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

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

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statement{
		ExtraStringOptions: make(map[string]string),
		ExtraBytesOptions:  make(map[string][]byte),
		ExtraIntOptions:    make(map[string]int64),
		ExtraDoubleOptions: make(map[string]float64),
	}, nil
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	return nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
	switch key {
	case ConnectionOptionStringOption1:
		return c.StringOption1, nil
	case ConnectionOptionStringOption2:
		return c.StringOption2, nil
	case ConnectionOptionStringOption3:
		return c.StringOption3, nil
	default:
		val, ok := c.ExtraStringOptions[key]
		if ok {
			return val, nil
		}
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown connection string type option `%s`", key),
		}
	}
}

func (c *connectionImpl) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case ConnectionOptionBytesOption1:
		return c.BytesOption1, nil
	case ConnectionOptionBytesOption2:
		return c.BytesOption2, nil
	case ConnectionOptionBytesOption3:
		return c.BytesOption3, nil
	default:
		val, ok := c.ExtraBytesOptions[key]
		if ok {
			return val, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown connection bytes type option `%s`", key),
		}
	}
}

func (c *connectionImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case ConnectionOptionIntOption1:
		return c.IntOption1, nil
	case ConnectionOptionIntOption2:
		return c.IntOption2, nil
	case ConnectionOptionIntOption3:
		return c.IntOption3, nil
	default:
		val, ok := c.ExtraIntOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown connection int type option `%s`", key),
		}
	}
}

func (c *connectionImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case ConnectionOptionDoubleOption1:
		return c.DoubleOption1, nil
	case ConnectionOptionDoubleOption2:
		return c.DoubleOption2, nil
	case ConnectionOptionDoubleOption3:
		return c.DoubleOption3, nil
	default:
		val, ok := c.ExtraDoubleOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown connection double type option `%s`", key),
		}
	}
}

func (c *connectionImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		v := v // copy into loop scope
		switch k {
		case ConnectionOptionStringOption1:
			c.StringOption1 = v
		case ConnectionOptionStringOption2:
			c.StringOption2 = v
		case ConnectionOptionStringOption3:
			c.StringOption3 = v
		default:
			err := c.checkExpectedError(k, "string")
			if err != nil {
				return err
			}
			c.ExtraStringOptions[k] = v
		}
	}
	return nil
}

func (c *connectionImpl) SetOption(key string, value string) error {
	switch key {
	case ConnectionOptionStringOption1:
		c.StringOption1 = value
	case ConnectionOptionStringOption2:
		c.StringOption2 = value
	case ConnectionOptionStringOption3:
		c.StringOption3 = value
	default:
		err := c.checkExpectedError(key, "string")
		if err != nil {
			return err
		}
		c.ExtraStringOptions[key] = value
	}
	return nil
}

func (c *connectionImpl) SetOptionBytes(key string, value []byte) error {
	switch key {
	case ConnectionOptionBytesOption1:
		c.BytesOption1 = value
	case ConnectionOptionBytesOption2:
		c.BytesOption2 = value
	case ConnectionOptionBytesOption3:
		c.BytesOption3 = value
	default:
		err := c.checkExpectedError(key, "bytes")
		if err != nil {
			return err
		}
		c.ExtraBytesOptions[key] = value
	}
	return nil
}

func (c *connectionImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case ConnectionOptionIntOption1:
		c.IntOption1 = value
	case ConnectionOptionIntOption2:
		c.IntOption2 = value
	case ConnectionOptionIntOption3:
		c.IntOption3 = value
	default:
		err := c.checkExpectedError(key, "int")
		if err != nil {
			return err
		}
		c.ExtraIntOptions[key] = value
	}
	return nil
}

func (c *connectionImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case ConnectionOptionDoubleOption1:
		c.DoubleOption1 = value
	case ConnectionOptionDoubleOption2:
		c.DoubleOption2 = value
	case ConnectionOptionDoubleOption3:
		c.DoubleOption3 = value
	default:
		err := c.checkExpectedError(key, "double")
		if err != nil {
			return err
		}
		c.ExtraDoubleOptions[key] = value
	}
	return nil
}

func (c *connectionImpl) checkExpectedError(key string, option_type string) error {
	if strings.Contains(key, "expected-error") {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown connection %s type option `%s`", option_type, key),
		}
	}
	return nil
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ReadPartition not yet implemented for mocks driver",
	}
}

// ListTableTypes implements driverbase.TableTypeLister.
func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"BASE TABLE", "TEMPORARY TABLE", "VIEW"}, nil
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return "", adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetCurrentCatalog not yet implemented for mocks driver",
	}
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return "", adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetCurrentDbSchema not yet implemented for mocks driver",
	}
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetCurrentCatalog not yet implemented for mocks driver",
	}
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetCurrentDbSchema not yet implemented for mocks driver",
	}
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetAutocommit not yet implemented for mocks driver",
	}
}

// Metadata methods
// Generally these methods return an array.RecordReader that
// can be consumed to retrieve metadata about the database as Arrow
// data. The returned metadata has an expected schema given in the
// doc strings of the specific methods. Schema fields are nullable
// unless otherwise marked. While no Statement is used in these
// methods, the result set may count as an active statement to the
// driver for the purposes of concurrency management (e.g. if the
// driver has a limit on concurrent active statements and it must
// execute a SQL query internally in order to implement the metadata
// method).
//
// Some methods accept "search pattern" arguments, which are strings
// that can contain the special character "%" to match zero or more
// characters, or "_" to match exactly one character. (See the
// documentation of DatabaseMetaData in JDBC or "Pattern Value Arguments"
// in the ODBC documentation.) Escaping is not currently supported.
// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	catalog_name								| utf8
//	catalog_db_schemas					| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	db_schema_name							| utf8
//	db_schema_tables						|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	table_name									| utf8 not null
//	table_type									|	utf8 not null
//	table_columns								| list<COLUMN_SCHEMA>
//	table_constraints						| list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//		Field Name 									| Field Type					| Comments
//		----------------------------|---------------------|---------
//		column_name									| utf8 not null				|
//		ordinal_position						| int32								| (1)
//		remarks											| utf8								| (2)
//		xdbc_data_type							| int16								| (3)
//		xdbc_type_name							| utf8								| (3)
//		xdbc_column_size						| int32								| (3)
//		xdbc_decimal_digits					| int16								| (3)
//		xdbc_num_prec_radix					| int16								| (3)
//		xdbc_nullable								| int16								| (3)
//		xdbc_column_def							| utf8								| (3)
//		xdbc_sql_data_type					| int16								| (3)
//		xdbc_datetime_sub						| int16								| (3)
//		xdbc_char_octet_length			| int32								| (3)
//		xdbc_is_nullable						| utf8								| (3)
//		xdbc_scope_catalog					| utf8								| (3)
//		xdbc_scope_schema						| utf8								| (3)
//		xdbc_scope_table						| utf8								| (3)
//		xdbc_is_autoincrement				| bool								| (3)
//		xdbc_is_generatedcolumn			| bool								| (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type					| Comments
//	----------------------------|---------------------|---------
//	constraint_name							| utf8								|
//	constraint_type							| utf8 not null				| (1)
//	constraint_column_names			| list<utf8> not null | (2)
//	constraint_column_usage			| list<USAGE_SCHEMA>	| (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name									|	Field Type
//	----------------------------|----------------------------
//	fk_catalog									| utf8
//	fk_db_schema								| utf8
//	fk_table										| utf8 not null
//	fk_column_name							| utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetObjects not yet implemented for mocks driver",
	}
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetTableSchema not yet implemented for mocks driver",
	}
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Commit not yet implemented for mocks driver",
	}
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(_ context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Rollback not yet implemented for mocks driver",
	}
}
