package mocks

import (
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/maps"
)

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

const (
	DatabaseOptionStringOption1 = "adbc.mocks.database.string_option_1"
	DatabaseOptionStringOption2 = "adbc.mocks.database.string_option_2"
	DatabaseOptionStringOption3 = "adbc.mocks.database.string_option_3"
	DatabaseOptionBytesOption1  = "adbc.mocks.database.bytes_option_1"
	DatabaseOptionBytesOption2  = "adbc.mocks.database.bytes_option_2"
	DatabaseOptionBytesOption3  = "adbc.mocks.database.bytes_option_3"
	DatabaseOptionIntOption1    = "adbc.mocks.database.int_option_1"
	DatabaseOptionIntOption2    = "adbc.mocks.database.int_option_2"
	DatabaseOptionIntOption3    = "adbc.mocks.database.int_option_3"
	DatabaseOptionDoubleOption1 = "adbc.mocks.database.double_option_1"
	DatabaseOptionDoubleOption2 = "adbc.mocks.database.double_option_2"
	DatabaseOptionDoubleOption3 = "adbc.mocks.database.double_option_3"

	ConnectionOptionStringOption1 = "adbc.mocks.connection.string_option_1"
	ConnectionOptionStringOption2 = "adbc.mocks.connection.string_option_2"
	ConnectionOptionStringOption3 = "adbc.mocks.connection.string_option_3"
	ConnectionOptionBytesOption1  = "adbc.mocks.connection.bytes_option_1"
	ConnectionOptionBytesOption2  = "adbc.mocks.connection.bytes_option_2"
	ConnectionOptionBytesOption3  = "adbc.mocks.connection.bytes_option_3"
	ConnectionOptionIntOption1    = "adbc.mocks.connection.int_option_1"
	ConnectionOptionIntOption2    = "adbc.mocks.connection.int_option_2"
	ConnectionOptionIntOption3    = "adbc.mocks.connection.int_option_3"
	ConnectionOptionDoubleOption1 = "adbc.mocks.connection.double_option_1"
	ConnectionOptionDoubleOption2 = "adbc.mocks.connection.double_option_2"
	ConnectionOptionDoubleOption3 = "adbc.mocks.connection.double_option_3"

	StatementOptionStringOption1 = "adbc.mocks.statement.string_option_1"
	StatementOptionStringOption2 = "adbc.mocks.statement.string_option_2"
	StatementOptionStringOption3 = "adbc.mocks.statement.string_option_3"
	StatementOptionBytesOption1  = "adbc.mocks.statement.bytes_option_1"
	StatementOptionBytesOption2  = "adbc.mocks.statement.bytes_option_2"
	StatementOptionBytesOption3  = "adbc.mocks.statement.bytes_option_3"
	StatementOptionIntOption1    = "adbc.mocks.statement.int_option_1"
	StatementOptionIntOption2    = "adbc.mocks.statement.int_option_2"
	StatementOptionIntOption3    = "adbc.mocks.statement.int_option_3"
	StatementOptionDoubleOption1 = "adbc.mocks.statement.double_option_1"
	StatementOptionDoubleOption2 = "adbc.mocks.statement.double_option_2"
	StatementOptionDoubleOption3 = "adbc.mocks.statement.double_option_3"
)

var (
	infoVendorVersion string
)

func init() {
	infoVendorVersion = "1.0.0"
}

type driverImpl struct {
	driverbase.DriverImplBase
}

// NewDriver creates a new BigQuery driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	info := driverbase.DefaultDriverInfo("mocks")
	if infoVendorVersion != "" {
		if err := info.RegisterInfoCode(adbc.InfoVendorVersion, infoVendorVersion); err != nil {
			panic(err)
		}
	}
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc)})
}

func (d *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	db := &databaseImpl{
		DatabaseImplBase:   driverbase.NewDatabaseImplBase(&d.DriverImplBase),
		ExtraStringOptions: make(map[string]string),
		ExtraBytesOptions:  make(map[string][]byte),
		ExtraIntOptions:    make(map[string]int64),
		ExtraDoubleOptions: make(map[string]float64),
	}
	if err := db.SetOptions(opts); err != nil {
		return nil, err
	}

	return driverbase.NewDatabase(db), nil
}
