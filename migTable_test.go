// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"testing"

	"github.com/jusongchen/mssql"
)

func init() {
	dbConn, err := mssql.NewConn("localhost", "ITSM_social", "", "")
	if err != nil {
		println("mssql.NewConn failed %v", err)
	}
	_ = dbConn
}

func TestmakeTask(t *testing.T) {
	r, err := makeTask(nil)
	t.Logf("makeTask return %v,%v", r, err)
}
