package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type IDGenerator struct {
	next int
	mu   sync.Mutex
}

var gen = IDGenerator{
	next: 0,
}

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		var counter int
		gen.mu.Lock()
		counter = gen.next
		gen.next += 1
		gen.mu.Unlock()

		// Update the message type to return back.
		body["type"] = "generate_ok"
		body["id"] = fmt.Sprintf("%v.%v.%v", time.Now().Unix(), n.ID(), counter)

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
