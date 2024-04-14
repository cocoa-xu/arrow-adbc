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
	"strings"
)

type databaseImpl struct {
	driverbase.DatabaseImplBase

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

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	conn := &connectionImpl{
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
		ExtraStringOptions: make(map[string]string),
		ExtraBytesOptions:  make(map[string][]byte),
		ExtraIntOptions:    make(map[string]int64),
		ExtraDoubleOptions: make(map[string]float64),
	}

	return driverbase.NewConnectionBuilder(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		WithTableTypeLister(conn).
		Connection(), nil
}

func (d *databaseImpl) Close() error {
	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case DatabaseOptionStringOption1:
		return d.StringOption1, nil
	case DatabaseOptionStringOption2:
		return d.StringOption2, nil
	case DatabaseOptionStringOption3:
		return d.StringOption3, nil
	default:
		val, ok := d.ExtraStringOptions[key]
		if ok {
			return val, nil
		}
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database string type option `%s`", key),
		}
	}
}

func (d *databaseImpl) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case DatabaseOptionBytesOption1:
		return d.BytesOption1, nil
	case DatabaseOptionBytesOption2:
		return d.BytesOption2, nil
	case DatabaseOptionBytesOption3:
		return d.BytesOption3, nil
	default:
		val, ok := d.ExtraBytesOptions[key]
		if ok {
			return val, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database bytes type option `%s`", key),
		}
	}
}

func (d *databaseImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case DatabaseOptionIntOption1:
		return d.IntOption1, nil
	case DatabaseOptionIntOption2:
		return d.IntOption2, nil
	case DatabaseOptionIntOption3:
		return d.IntOption3, nil
	default:
		val, ok := d.ExtraIntOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database int type option `%s`", key),
		}
	}
}

func (d *databaseImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case DatabaseOptionDoubleOption1:
		return d.DoubleOption1, nil
	case DatabaseOptionDoubleOption2:
		return d.DoubleOption2, nil
	case DatabaseOptionDoubleOption3:
		return d.DoubleOption3, nil
	default:
		val, ok := d.ExtraDoubleOptions[key]
		if ok {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database double type option `%s`", key),
		}
	}
}

func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		v := v // copy into loop scope
		switch k {
		case DatabaseOptionStringOption1:
			d.StringOption1 = v
		case DatabaseOptionStringOption2:
			d.StringOption2 = v
		case DatabaseOptionStringOption3:
			d.StringOption3 = v
		default:
			err := d.checkExpectedError(k, "string")
			if err != nil {
				return err
			}
			d.ExtraStringOptions[k] = v
		}
	}
	return nil
}

func (d *databaseImpl) SetOption(key string, value string) error {
	switch key {
	case DatabaseOptionStringOption1:
		d.StringOption1 = value
	case DatabaseOptionStringOption2:
		d.StringOption2 = value
	case DatabaseOptionStringOption3:
		d.StringOption3 = value
	default:
		err := d.checkExpectedError(key, "string")
		if err != nil {
			return err
		}
		d.ExtraStringOptions[key] = value
	}
	return nil
}

func (d *databaseImpl) SetOptionBytes(key string, value []byte) error {
	switch key {
	case DatabaseOptionBytesOption1:
		d.BytesOption1 = value
	case DatabaseOptionBytesOption2:
		d.BytesOption2 = value
	case DatabaseOptionBytesOption3:
		d.BytesOption3 = value
	default:
		err := d.checkExpectedError(key, "bytes")
		if err != nil {
			return err
		}
		d.ExtraBytesOptions[key] = value
	}
	return nil
}

func (d *databaseImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case DatabaseOptionIntOption1:
		d.IntOption1 = value
	case DatabaseOptionIntOption2:
		d.IntOption2 = value
	case DatabaseOptionIntOption3:
		d.IntOption3 = value
	default:
		err := d.checkExpectedError(key, "int")
		if err != nil {
			return err
		}
		d.ExtraIntOptions[key] = value
	}
	return nil
}

func (d *databaseImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case DatabaseOptionDoubleOption1:
		d.DoubleOption1 = value
	case DatabaseOptionDoubleOption2:
		d.DoubleOption2 = value
	case DatabaseOptionDoubleOption3:
		d.DoubleOption3 = value
	default:
		err := d.checkExpectedError(key, "double")
		if err != nil {
			return err
		}
		d.ExtraDoubleOptions[key] = value
	}
	return nil
}

func (d *databaseImpl) checkExpectedError(key string, option_type string) error {
	if strings.Contains(key, "expected-error") {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown database %s type option `%s`", option_type, key),
		}
	}
	return nil
}
