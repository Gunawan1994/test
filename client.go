package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"net"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/message_queue")
	if err == nil {
		db.Exec("CREATE DATABASE IF NOT EXISTS message_queue")
	}
	for {
		conn, _ := net.Dial("tcp", "127.0.0.1:8000")
		fmt.Print("insert message: ")
		input := bufio.NewScanner(os.Stdin)
		input.Scan()
		fmt.Println(input.Text())

		sql := "INSERT INTO message(message_txt) VALUES(?)"

		stmt, err := db.Prepare(sql)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		result, err2 := stmt.Exec(input.Text() + "\n")
		if err2 != nil {
			panic(err2)
		}

		result.LastInsertId()

		fmt.Fprintf(conn, input.Text()+"\n")
		defer conn.Close()
	}

}
