// Copyright 2015 Jusong Chen
//
// Author: Jusong Chen (jusong.chen@gmail.com)

package main

import (
	"database/sql"
	"time"

	log "github.com/golang/glog"
	"github.com/jusongchen/mssql"
)

type tuple []interface{}

//Done() is executed as last step in parallel execution
func (o tuple) Done() {
	//log.Fatalf("%s %s.%s processed", o.TypeDesc, o.SchemaName, o.ObjName)
	//log.Fatalf("row with SchemaName %s processed", o.SchemaName)
	log.Infof("Processed:%+v", o)
}

//Process()
//TODO:create cockroach table
func (o tuple) Process() {
	log.Infof("Processing %+v", o)

	log.Infof("col1 %+v %+v", o[0], o[1])
	// switch o.TypeDesc {
	// case "USER_TABLE":
	// 	//TODO
	// 	t := DbTable{DbObj: *o}
	// 	t.Process()
	// default:
	// 	log.Infof("Process %+v", *o)
	// }
}

//Make() create SchemaObj which implements interface Task
// func (f *SchemaObjRows) Make() (Task, error) {

// 	o := DbObj{}
// 	err := f.Scan(&o.ObjectID, &o.ObjName, &o.SchemaName, &o.TypeDesc, &o.CreateDate, &o.ModifyDate)

// 	return &o, err
// }

//a row is represented as a slice of columns

type DbObj struct {
	ObjectID   int64
	ObjName    string
	SchemaName string
	TypeDesc   string
	CreateDate time.Time
	ModifyDate time.Time
}

//Done() is executed as last step in parallel execution
func (o *DbObj) Done() {
	//log.Fatalf("%s %s.%s processed", o.TypeDesc, o.SchemaName, o.ObjName)
	//log.Fatalf("row with SchemaName %s processed", o.SchemaName)
	log.Infof("Processed:%+v", *o)
}

//Process()
//TODO:create cockroach table
func (o *DbObj) Process() {
	log.Infof("Processing %+v", *o)

	// switch o.TypeDesc {
	// case "USER_TABLE":
	// 	//TODO
	// 	t := DbTable{DbObj: *o}
	// 	t.Process()
	// default:
	// 	log.Infof("Process %+v", *o)
	// }
}

// func tmp_migTables(dbConn *mssql.Conn, dop int) error {

// 	var f qrySchemaFunc

// 	f = func(db *sql.DB) (*sql.Rows, error) {
// 		return db.Query(
// 			`SELECT OBJECT_ID
// 				,name AS object_name
// 				,SCHEMA_NAME(schema_id) AS schema_name
// 				,type_desc
// 				,create_date
// 				,modify_date
// 			FROM sys.objects
// 			WHERE type_desc='USER_TABLE'
// 			order by schema_name, type_desc, object_name`)
// 	}

// 	err := RunQuery(dop, dbConn.DB, f, )

// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	return err
// }

func makeSchemaObj(r *sql.Rows) mssql.Task {
	o := DbObj{}

	err := r.Scan(&o.ObjectID, &o.ObjName, &o.SchemaName, &o.TypeDesc, &o.CreateDate, &o.ModifyDate)
	if err != nil {
		log.Fatal(err)
	}
	return &o
}

//migTables get table definitions from source DB and for each table:
//		1.create destination table in cockroach
//		2. copy data from source to dest
//one table is processed in one goroutine while multiple tables can be processed concurrently

func migTables(dbConn *mssql.Conn, DOP int) error {

	rs, err := dbConn.DB.Query(
		`SELECT OBJECT_ID	
				,name AS object_name
				,SCHEMA_NAME(schema_id) AS schema_name
				,type_desc
				,create_date
				,modify_date
			FROM sys.objects
			WHERE type_desc='USER_TABLE'
			order by schema_name, type_desc, object_name`)

	if err != nil {
		log.Fatal(err)
		return err
	}
	// close Query
	defer func() {
		rs.Close()
	}()

	// err := RunQuery(DOP, dbConn.DB, f, )
	// ExecQuery(DOP int, rs *sql.Rows, f MakeTaskFunc)
	mssql.ExecQuery(DOP, rs, makeSchemaObj)

	return nil
}

// func (f *SchemaObjRows) Make() (Task, error) {
// 	var r tuple

// 	for i := 0; i < 4; i++ {
// 		//create six fields
// 		colValue := make([]byte, 2000)
// 		r = append(r, &colValue)

// 	}

// 	err := f.Scan(r...)

// 	return &r, err
// }
