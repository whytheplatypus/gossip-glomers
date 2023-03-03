package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastMsg struct {
	Type string `json:"type"`
	Msg  int    `json:"message"`
}

type topologyMsg struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type readMsg struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

func main() {
	neighbors := []string{}
	n := maelstrom.NewNode()
	seen := map[int]struct{}{}
	var l sync.Mutex
	c := make(chan int)
	go func() {
		for i := range c {
			l.Lock()
			seen[i] = struct{}{}
			l.Unlock()
		}
	}()

	//local := map[int]struct{}{}

	r := regexp.MustCompile("c[0-9]+")

	ticker := time.NewTicker(200 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				for _, neighbor := range neighbors {
					n.Send(neighbor, &readMsg{
						Type: "read",
					})
				}
			}
		}
	}()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := &broadcastMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		c <- body.Msg

		if r.MatchString(msg.Src) {
			resp := map[string]string{"type": "broadcast_ok"}
			return n.Reply(msg, resp)
		}
		return nil
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		body := &topologyMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		gotNeighbors, ok := body.Topology[n.ID()]
		if !ok {
			fmt.Errorf("failed to build topology for %s", n.ID())
		}
		neighbors = gotNeighbors

		resp := map[string]string{"type": "topology_ok"}
		return n.Reply(msg, resp)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := &readMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}
		body.Type = "read_ok"
		s := []int{}
		l.Lock()
		for k := range seen {
			s = append(s, k)
		}
		l.Unlock()
		body.Messages = s
		return n.Reply(msg, body)
	})

	n.Handle("read_ok", func(msg maelstrom.Message) error {
		body := &readMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}
		for _, msg := range body.Messages {
			c <- msg
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
