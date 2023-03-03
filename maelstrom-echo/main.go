package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type echoMsg struct {
	Type  string `json:"type"`
	MsgId int    `json:"msg_id"`
	Echo  string `json:"echo"`
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		body := &echoMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		body.Type = "echo_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
