package main

import (
	"context"
	"encoding/json"
	"log"

	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type addMsg struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type readMsg struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func main() {
	var count atomic.Int64
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		body := &addMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}

		ctx := context.Background()
		c, err := increment(ctx, int(count.Load()), body.Delta, kv)
		if err != nil {
			return err
		}

		count.Swap(int64(c))
		log.Println(count.Load())

		return n.Reply(msg, map[string]string{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := &readMsg{}
		if err := json.Unmarshal(msg.Body, body); err != nil {
			return err
		}
		c, err := increment(context.Background(), int(count.Load()), 1, kv)
		if err != nil {
			return err
		}
		cc, e := increment(context.Background(), c, -1, kv)
		if e != nil {
			return e
		}

		count.Swap(int64(cc))

		body.Type = "read_ok"
		body.Value = int(count.Load())
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

const countKey = "count"

// add context?
func increment(ctx context.Context, count, delta int, kv *maelstrom.KV) (int, error) {
	newCount := count + delta
	if err := kv.CompareAndSwap(ctx, countKey, count, newCount, true); err != nil {
		rpcErr, ok := err.(*maelstrom.RPCError)
		if ok && rpcErr.Code == maelstrom.PreconditionFailed {
			//we're out of sync.
			c, e := kv.ReadInt(ctx, countKey)
			if e != nil {
				return c, e
			}
			return increment(ctx, c, delta, kv)
		}
		return 0, err
	}
	return newCount, nil
}
