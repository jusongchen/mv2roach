//Copyright 2015 Jusong Chen
//
//// Author:  Jusong Chen (jusong.chen@gmail.com)

package main

import (
	"flag"
	"runtime"

	log "github.com/golang/glog"
	"github.com/jusongchen/mssql"
)

var (
	mssrv  = flag.String("srv", "localhost", "hostname of MS SQL server; use hostname\\instance_name to connect to a named instance")
	msdb   = flag.String("msdb", "", "ms sql server database name")
	msuser = flag.String("user", "", "user name to login the sql server instance; trusted connection is used if not specified ")
	mspass = flag.String("passwd", "", "password of the SQL server user")
	dop    = flag.Int("DOP", 4*runtime.NumCPU(), " Degree of Concurrency/Parallelism")
	//interval = flag.Duration("i", 5*time.Second, "interval between each ping")

	//port = flag.String("port", "80", "web server port number")
)

// var dbConn *mssql.Conn

func main() {
	flag.Parse() //   SetupDB()

	dbConn, err := mssql.NewConn(*mssrv, *msdb, *msuser, *mspass)

	if dbConn == nil || err != nil {
		log.Fatal(err)
	}
	if log.V(2) {
		log.Infof("DOP %d", *dop)
		log.Flush()
	}
	migTables(dbConn, *dop)
	log.Flush()
}
