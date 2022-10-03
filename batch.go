// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package chbuf

import "github.com/yusufozturk/chbuf/proto"

type Batch struct {
	Err   error
	Block *proto.Block
	Map   structMap
}

func (b *Batch) Append(v ...interface{}) error {
	if err := b.Block.Append(v...); err != nil {
		return err
	}
	return nil
}

func (b *Batch) AppendStruct(v interface{}) error {
	values, err := b.Map.Map("AppendStruct", b.Block.ColumnsNames(), v, false)
	if err != nil {
		return err
	}
	return b.Append(values...)
}
