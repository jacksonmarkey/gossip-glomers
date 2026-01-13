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
	buffer   []int
}

var broadcastList = BroadcastList{
	seen: make(map[int]struct{}),
	ctx:  context.Background(),
}

var efficientTopology = map[string][]string{
	"n0":  []string{"n1", "n2"},
	"n1":  []string{"n3", "n4"},
	"n2":  []string{"n5", "n6"},
	"n3":  []string{"n7", "n8"},
	"n4":  []string{"n9", "n10"},
	"n5":  []string{"n11", "n12"},
	"n6":  []string{"n13", "n14"},
	"n7":  []string{"n15", "n16"},
	"n8":  []string{"n17", "n18"},
	"n9":  []string{"n19", "n20"},
	"n10": []string{"n21", "n22"},
	"n11": []string{"n23", "n24"},
	"n12": []string{},
	"n13": []string{},
	"n14": []string{},
	"n15": []string{},
	"n16": []string{},
	"n17": []string{},
	"n18": []string{},
	"n19": []string{},
	"n20": []string{},
	"n21": []string{},
	"n22": []string{},
	"n23": []string{},
	"n24": []string{},
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var msgBody, replyBody map[string]any
		if err := json.Unmarshal(msg.Body, &msgBody); err != nil {
			return err
		}
		if n.ID() == "n0" {
			broadcastList.mu.Lock()
			broadcastList.buffer = append(broadcastList.buffer, int(msgBody["message"].(float64)))
			broadcastList.mu.Unlock()
		} else {
			if _, ok := msgBody["propogated"]; !ok {
				neighbor := "n0"
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
			} else {
				buffer := msgBody["buffer"].([]any)
				broadcastList.mu.Lock()
				for _, x := range buffer {
					broadcastList.seen[int(x.(float64))] = struct{}{}
				}

				neighbors := efficientTopology[n.ID()]
				for _, neighbor := range neighbors {
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
				broadcastList.mu.Unlock()
			}
		}
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

	go bufferedBroadcast(n)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func bufferedBroadcast(n *maelstrom.Node) {
	// Once Node.ID() is initialized, exit if we aren't the root node of the tree's topology
	for {
		if len(n.ID()) > 0 {
			if n.ID() == "n0" {
				break
			} else {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Intermittently flush the root node's buffer
	for {
		broadcastList.mu.Lock()
		if len(broadcastList.buffer) > 0 {
			for _, x := range broadcastList.buffer {
				broadcastList.seen[x] = struct{}{}
			}
			msgBody := make(map[string]any)
			msgBody["type"] = "broadcast"
			msgBody["buffer"] = broadcastList.buffer
			msgBody["propogated"] = struct{}{}
			neighbors := efficientTopology[n.ID()]

			for _, neighbor := range neighbors {
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
			broadcastList.buffer = make([]int, 0)
		}
		broadcastList.mu.Unlock()
		time.Sleep(300 * time.Millisecond)
	}
}
