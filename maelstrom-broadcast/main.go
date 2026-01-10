package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastList struct {
	seen     map[int]struct{}
	neigbors []string
	mu       sync.Mutex
}

var broadcastList = BroadcastList{
	seen: make(map[int]struct{}),
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}
		i := int(msgBody["message"].(float64))

		// If we've seen the value before, just return
		// If we haven't seen it before, record it and send it to all neighbors
		broadcastList.mu.Lock()
		_, alreadySeen := broadcastList.seen[i]
		if !alreadySeen {
			broadcastList.seen[i] = struct{}{}
			// TODO: tradeoffs of keeping the lock while we send these RPCs
			// 1. According to the documentation, Node.Send() shouldn't wait for a response,
			// so in theory we don't even need to send each in a new goroutine.
			// 2. We don't want to accidentally change the topology while we're iterating over it.
			// An alternative would be to copy the topology and iterate over the copy
			// so we can release the lock earlier, if it's hanging too long.
			for _, neighbor := range broadcastList.neigbors {
				if neighbor == n.ID() || neighbor == msg.Src {
					continue
				}
				go func(recipient string) {
					if err := n.Send(recipient, msgBody); err != nil {
						panic(err)
					}
				}(neighbor)
			}
		}
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

		// Convert set representation of recorded messages to slice
		broadcastList.mu.Lock()
		seen := broadcastList.seen
		messages := make([]int, len(seen))
		i := 0
		for x := range seen {
			messages[i] = x
			i++
		}
		broadcastList.mu.Unlock()

		// Construct reply
		replyBody = make(map[string]any)
		replyBody["messages"] = messages
		replyBody["type"] = "read_ok"
		return n.Reply(msg, replyBody)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}

		// Lots of type casting the unmarshalled JSON
		topology := msgBody["topology"].(map[string]any)
		neighbors := topology[n.ID()].([]any)

		broadcastList.mu.Lock()
		broadcastList.neigbors = make([]string, len(neighbors))
		for i, n := range neighbors {
			broadcastList.neigbors[i] = n.(string)
		}
		broadcastList.mu.Unlock()

		// Construct reply
		replyBody = make(map[string]any)
		replyBody["type"] = "topology_ok"
		return n.Reply(msg, replyBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
