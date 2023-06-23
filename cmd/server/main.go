package main

import (
	"github.com/travisjeffery/proglog/internal/server"
	"log"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatalln(srv.ListenAndServe())
}
