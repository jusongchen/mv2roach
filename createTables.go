// Copyright 2015 Jusong Chen
//
// Author: Jusong Chen (jusong.chen@gmail.com)

package main

import (
	"time"

	log "github.com/golang/glog"
	"github.com/jusongchen/mssql"
)

type SchemaObj struct {
	ObjectID   int64
	ObjName    string
	SchemaName string
	TypeDesc   string
	CreateDate time.Time
	ModifyDate time.Time
}

//Done() is executed as last step in parallel execution
func (o *SchemaObj) Done() {
	//log.Fatalf("%s %s.%s processed", o.TypeDesc, o.SchemaName, o.ObjName)
	//log.Fatalf("row with SchemaName %s processed", o.SchemaName)
	log.Infof("Processed:%+v", *o)
}

//Process()
//TODO:create cockroach table
func (o *SchemaObj) Process() {
	//fmt.Fprintf(w, "%s %s.%s is being processing", o.TypeDesc, o.SchemaName, o.ObjName)
	//fmt.Fatalf("%s %s.%s is being processing", o.TypeDesc, o.SchemaName, o.ObjName)
	log.Infof("processing:%+v", *o)

}

//rsTable Serves as the factory for parallel run
type SchemaObjRows struct {
	*mssql.Rowset
}

func (f *SchemaObjRows) Open() error {
	//rowset already open
	return nil
}

//Make() create SchemaObj which implements interface task
func (f *SchemaObjRows) Make() (task, error) {

	o := SchemaObj{}
	err := f.Scan(&o.ObjectID, &o.ObjName, &o.SchemaName, &o.TypeDesc, &o.CreateDate, &o.ModifyDate)

	return &o, err
}

//
//CreateTables get table definition from source DB and create them in cockroach
func CreateTables(conn *mssql.Conn, dop int) {
	var sqlText = `SELECT OBJECT_ID	
	,name AS object_name
	,SCHEMA_NAME(schema_id) AS schema_name
	,type_desc
	,create_date
	,modify_date
FROM sys.objects
WHERE type_desc=?
order by schema_name, type_desc, object_name`

	if err := mssql.OpenDB(conn); err != nil {
		log.Fatal(err)
	}

	//time.Sleep(time.Minute)

	rs, err := mssql.OpenRowSet(conn, sqlText, "USER_TABLE")
	if err != nil {
		log.Fatal(err)
	}

	f := SchemaObjRows{rs}

	runInParallel(
		&f,
		dop)

}

/*

Description of the object type:
AGGREGATE_FUNCTION
CHECK_CONSTRAINT
CLR_SCALAR_FUNCTION
CLR_STORED_PROCEDURE
CLR_TABLE_VALUED_FUNCTION
CLR_TRIGGER
DEFAULT_CONSTRAINT
EXTENDED_STORED_PROCEDURE
FOREIGN_KEY_CONSTRAINT
INTERNAL_TABLE
PLAN_GUIDE
PRIMARY_KEY_CONSTRAINT
REPLICATION_FILTER_PROCEDURE
RULE
SEQUENCE_OBJECT
SERVICE_QUEUE
SQL_INLINE_TABLE_VALUED_FUNCTION
SQL_SCALAR_FUNCTION
SQL_STORED_PROCEDURE
SQL_TABLE_VALUED_FUNCTION
SQL_TRIGGER
SYNONYM
SYSTEM_TABLE
TABLE_TYPE
UNIQUE_CONSTRAINT
USER_TABLE
VIEW

*/

//Query to get all columns of user tables
var SqlGetColumn = `
SELECT
OBJECT_NAME(c.OBJECT_ID) TableName
,c.name AS ColumnName
,c.column_id
,SCHEMA_NAME(o.schema_id) AS SchemaName
,t.name AS TypeName
,t.user_type_id
,t.is_user_defined
,t.is_assembly_type
,c.max_length
,c.PRECISION
,c.scale
--into tmp_ntext_fields
FROM sys.columns AS c
JOIN sys.types AS t ON c.user_type_id=t.user_type_id
join sys.objects o on c.object_id = o.object_id 
where
--t.name in ('text','ntext')
--and 
o.type = 'U'  -- user table only
ORDER BY TableName;
`
