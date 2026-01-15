package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const COUNTKEY = "count"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := int(body["delta"].(float64))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for {
			count, err := kv.ReadInt(ctx, COUNTKEY)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				count = 0
			}
			updatedCount := count + delta
			err = kv.CompareAndSwap(ctx, COUNTKEY, count, updatedCount, true)
			if err == nil || maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				break
			}
		}

		// Update the message type to return back.
		replyBody = make(map[string]any)
		replyBody["type"] = "add_ok"
		return n.Reply(msg, replyBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		count, err := kv.ReadInt(ctx, COUNTKEY)
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			count = 0
		}

		// Update the message type to return back.
		replyBody = make(map[string]any)
		replyBody["type"] = "read_ok"
		replyBody["value"] = count
		return n.Reply(msg, replyBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
