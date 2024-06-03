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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
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

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, parameters array.RecordReader, conn *connectionImpl, query string, resultRecordBufferSize, prefetchConcurrency int) (rdr *reader, totalRows int64, err error) {
	alloc := conn.Alloc
	bodyReader := bytes.NewReader([]byte(query))
	req, err := http.NewRequest(http.MethodPost, conn.address, bodyReader)
	if err != nil {
		return nil, -1, err
	}

	auth := conn.username + ":" + conn.password
	encodedAuth := base64.StdEncoding.EncodeToString([]byte(auth))

	req.Header.Add("Authorization", "Basic "+encodedAuth)

	var tr *http.Transport = nil
	if strings.HasPrefix(conn.address, "https://") {
		tr = &http.Transport{
			// todo: allow user to change these settings if we choose to use this approach
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if err != nil {
		return nil, -1, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, -1, err
		}
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("status code: %d, message: %s", res.StatusCode, string(b)),
		}
	}

	// todo: the following doesn't work
	// will investigate if we choose to use this approach
	//
	//   `ipc.NewReader(res.Body, ipc.WithAllocator(alloc))`
	//
	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, -1, err
	}
	ipcRdr, err := ipc.NewReader(bytes.NewReader(b), ipc.WithAllocator(alloc))
	if err != nil {
		return nil, -1, err
	}

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
		schema:   ipcRdr.Schema(),
	}

	go func() {
		defer ipcRdr.Release()
		for ipcRdr.Next() && ctx.Err() == nil {
			fmt.Printf("read?\n")
			rec := ipcRdr.Record()
			rec.Retain()
			ch0 <- rec
		}

		fmt.Printf("close?\n")
		rdr.err = checkContext(ctx, rdr.Err())
		defer close(ch0)
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
