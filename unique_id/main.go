package main

import (
	"encoding/json"
	"fmt"
	"log"

	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type generateMsg struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

func main() {
	count := &atomic.Int64{}
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		body := &generateMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		body.ID = fmt.Sprintf("%s:%d", n.ID(), count.Add(1))

		body.Type = "generate_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
