// Copyright 2015 Jusong Chen
//
// Author: Jusong Chen (jusong.chen@gmail.com)

package main

import log "github.com/golang/glog"

type DbTable struct {
	DbObj
}

//
//copyData refer to dbConn a global variable defined in main.go
func (t *DbTable) copyData() error {

	// sqlText := fmt.Sprintf(`SELECT * FROM %s.%s`, t.SchemaName, t.ObjName)

	// //time.Sleep(time.Minute)

	// rs, err := mssql.OpenRowSet(dbConn, sqlText)
	// defer func() {
	// 	rs.Close()
	// }()

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// var r []byte

	// for rs.Next() {
	// 	rs.Scan(&r)
	// }

	// fmt.Printf("row data %v", r)

	return nil
}

func (t *DbTable) Process() {

	//create cockroach table first
	//TODO create destination table
	// destTableName, err := createDestTable()

	// if err != nil {
	// 	log.Fatal(err)
	// }

	//copy Data from source to dest

	t.copyData()

	log.Infof("processing table:%+v", t)

}
