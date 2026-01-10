package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastList struct {
	seen []int
	mu   sync.Mutex
}

var broadcastList = BroadcastList{
	seen: make([]int, 0),
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}
		i := int(msgBody["message"].(float64))
		broadcastList.mu.Lock()
		broadcastList.seen = append(broadcastList.seen, i)
		broadcastList.mu.Unlock()

		// Construct reply
		replyBody = make(map[string]any)
		replyBody["type"] = "broadcast_ok"
		return n.Reply(msg, replyBody)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}

		// Currently gets a reference to slice
		// TODO: perhaps copy the slice instead
		broadcastList.mu.Lock()
		seen := broadcastList.seen
		broadcastList.mu.Unlock()

		// Construct reply
		replyBody = make(map[string]any)
		replyBody["messages"] = seen
		replyBody["type"] = "read_ok"
		return n.Reply(msg, replyBody)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}

		// Construct reply
		replyBody = make(map[string]any)
		replyBody["type"] = "topology_ok"
		return n.Reply(msg, replyBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
