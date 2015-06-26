//Copyright 2015 Jusong Chen
//
//// Author:  Jusong Chen (jusong.chen@gmail.com)

package main

import (
	"sync"

	log "github.com/golang/glog"
)

type task interface {
	Process()
	Done()
}

type taskMaker interface {
	//open(db *sql.DB, sqlText string, args ...interface{}) (*sql.Rows, error)
	Open() error
	Close() error
	Next() bool
	Make() (task, error)
}

func runInParallel(m taskMaker, DOP int) {
	var wg sync.WaitGroup

	in := make(chan task)

	wg.Add(1)
	go func() {
		if err := m.Open(); err != nil {
			log.Fatal(err)
		}

		for m.Next() {
			if t, err := m.Make(); err == nil {
				in <- t
			} else {
				log.Fatal(err)
			}
		}
		close(in)
		wg.Done()
	}()

	out := make(chan task)

	for i := 0; i < DOP; i++ {
		wg.Add(1)
		go func() {
			for t := range in {
				t.Process()
				out <- t
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(out)

	}()

	for t := range out {
		t.Done()
	}

	if err := m.Close(); err != nil {
		log.Fatal(err)
	}
}
