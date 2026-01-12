package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastList struct {
	seen     map[int]struct{}
	neigbors []string
	mu       sync.Mutex
	ctx      context.Context
}

var broadcastList = BroadcastList{
	seen: make(map[int]struct{}),
	ctx:  context.Background(),
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
			for _, neighbor := range broadcastList.neigbors {
				if neighbor == n.ID() || neighbor == msg.Src {
					continue
				}
				go func(recipient string) {
					// repeatedly attempt send at intervals until success
					for {
						ctx, cancel := context.WithDeadline(broadcastList.ctx, time.Now().Add(1*time.Second))
						defer cancel()
						if _, err := n.SyncRPC(ctx, recipient, msgBody); err == nil {
							return
						}
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
