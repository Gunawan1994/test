package main

import (
	"database/sql"
	"fmt"
	"net"

	_ "github.com/go-sql-driver/mysql"
)

var messages = []string{}

type Task struct {
	Msg string `json:"message_txt"`
}

func Readmsg(msg string) {
	messages = append(messages, msg)
}

type Consumer struct {
	msgs *chan int
}

func NewConsumer(msgs *chan int) *Consumer {
	return &Consumer{msgs: msgs}
}

func (c *Consumer) consume() {
	fmt.Println("consumer: Started")

	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/message_queue")
	if err == nil {
		db.Exec("CREATE DATABASE IF NOT EXISTS message_queue")
	}
	migrate(db)
	sl, err := db.Query("SELECT * FROM message")
	if err != nil {
		panic(err.Error())
	}
	task := Task{}
	for sl.Next() {
		err := sl.Scan(&task.Msg)
		if err != nil {
			panic(err.Error())
		}
		fmt.Println("consumer: Message:", task.Msg)

	}

	for {
		msg := <-*c.msgs
		fmt.Println("consumer: Message:", messages[msg])
	}
}

type Producer struct {
	msgs *chan int
	done *chan bool
}

func NewProducer(msgs *chan int, done *chan bool) *Producer {
	return &Producer{msgs: msgs, done: done}
}

func (p *Producer) produce(max int) {
	ln, _ := net.Listen("tcp", ":8000")
	for i := 0; i < max; i++ {
		conn, _ := ln.Accept()
		var cmd []byte
		fmt.Fscan(conn, &cmd)
		Readmsg(string(cmd))
		*p.msgs <- i
	}
	*p.done <- true
	fmt.Println("produce: Done")
}

func main() {
	max := 5

	var msgs = make(chan int)
	var done = make(chan bool)

	go NewProducer(&msgs, &done).produce(max)

	go NewConsumer(&msgs).consume()

	<-done

}

func migrate(db *sql.DB) {
	sql := `
    CREATE TABLE IF NOT EXISTS message(
        queue_id VARCHAR(20) NOT NULL,
		message_txt VARCHAR(50) NOT NULL,
		status TINYINT
    );
    `
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}
