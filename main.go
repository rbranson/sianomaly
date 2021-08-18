package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type account struct {
	Name    string
	Balance int
}

type queryRow interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

type exec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func readAccount(qr queryRow, name string) account {
	acc := account{}
	row := qr.QueryRow("select * from accounts where name = ?", name)
	err := row.Scan(&acc.Name, &acc.Balance)
	if err != nil {
		panic(err)
	}
	fmt.Printf("R(%v)=%v\n", acc.Name, acc.Balance)
	return acc
}

func writeAccount(ex exec, name string, balance int) {
	_, err := ex.Exec("update accounts set balance = ? where name = ?", balance, name)
	if err != nil {
		panic(err)
	}
	fmt.Printf("W(%v)=%v\n", name, balance)
}

func main() {
	_ = os.Remove("sianomaly.db")

	db, err := sql.Open("sqlite3", "sianomaly.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	execOrPanic := func(q string) {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// allows concurrent transactions, otherwise everything will hang
	execOrPanic("pragma journal_mode=wal;")

	execOrPanic("create table accounts (name varchar not null primary key, balance integer not null);")
	execOrPanic("insert into accounts values('checking', 0);")
	execOrPanic("insert into accounts values('savings', 0);")

	tx1, err := db.Begin()
	if err != nil {
		panic(err)
	}

	tx2, err := db.Begin()
	if err != nil {
		panic(err)
	}

	// R2(X0,0)
	tx2accX := readAccount(tx2, "checking")
	// R2(X0,0)
	_ = readAccount(tx2, "savings")

	// R1(Y0,0)
	tx1accY := readAccount(tx1, "savings")
	// W1(Y1, 20)
	writeAccount(tx1, "savings", tx1accY.Balance+20)
	// C1
	err = tx1.Commit()
	if err != nil {
		panic(err)
	}

	tx3, err := db.Begin()
	if err != nil {
		panic(err)
	}
	// R3(X0,0)
	_ = readAccount(tx3, "checking")
	// R3(Y1,20)
	_ = readAccount(tx3, "savings")

	// C3
	err = tx3.Commit()
	if err != nil {
		panic(err)
	}

	// W2(X2,-11)
	writeAccount(tx2, "checking", tx2accX.Balance-10-1)
	// C2
	err = tx2.Commit()
	if err != nil {
		panic(err)
	}
}
