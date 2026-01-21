package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var n *maelstrom.Node
var ctx context.Context

var commitIndices *maelstrom.KV
var nextIndices *maelstrom.KV
var entries *maelstrom.KV

func main() {
	n = maelstrom.NewNode()
	ctx = context.Background()
	commitIndices = maelstrom.NewLinKV(n)
	nextIndices = maelstrom.NewLinKV(n)
	entries = maelstrom.NewSeqKV(n)

	n.Handle("send", sendHandler)
	n.Handle("poll", pollHandler)
	n.Handle("commit_offsets", commitOffsetsHandler)
	n.Handle("list_committed_offsets", listCommittedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func sendHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	logMsg := int(body["msg"].(float64))

	var lastOffset, nextOffset int
	var err error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		// 1 .Get unique offset
		lastOffset, err = nextIndices.ReadInt(ctx, key)
		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			lastOffset = 0
		}
		nextOffset = lastOffset + 1
		err = nextIndices.CompareAndSwap(ctx, key, lastOffset, nextOffset, true)
		if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
			// Our offset is stale, try again from the start
			continue
		}
		// 2. Upload our message under the key-offset composite key
		keyWithOffset := fmt.Sprintf("%v_%v", key, nextOffset)
		err = entries.Write(ctx, keyWithOffset, logMsg)
		break
	}

	replyBody = make(map[string]any)
	replyBody["type"] = "send_ok"
	replyBody["offset"] = nextOffset
	return n.Reply(msg, replyBody)
}

func pollHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	returnMessages := make(map[string][]any)
	offsets := body["offsets"].(map[string]any)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for key, o := range offsets {
		logMessagesPastOffset := make([]any, 0, 10)
		offset := int(o.(float64))

		for i := 0; i < 10; i++ {
			entryMsg, err := entries.ReadInt(ctx, fmt.Sprintf("%v_%v", key, offset+i))
			if err == nil {
				logMessagesPastOffset = append(logMessagesPastOffset, []int{offset + i, entryMsg})
			}
		}
		returnMessages[key] = logMessagesPastOffset
	}

	replyBody = make(map[string]any)
	replyBody["type"] = "poll_ok"
	replyBody["msgs"] = returnMessages
	return n.Reply(msg, replyBody)
}

func commitOffsetsHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	offsets := body["offsets"].(map[string]any)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for key, o := range offsets {
		newOffset := int(o.(float64))
		for {
			existingOffset, err := commitIndices.ReadInt(ctx, key)
			if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
				existingOffset = 0
			}
			if existingOffset >= newOffset {
				break
			}
			err = commitIndices.CompareAndSwap(ctx, key, existingOffset, newOffset, true)
			if maelstrom.ErrorCode(err) == maelstrom.PreconditionFailed {
				continue
			}
			break
		}
	}

	replyBody = make(map[string]any)
	replyBody["type"] = "commit_offsets_ok"
	return n.Reply(msg, replyBody)
}

func listCommittedOffsetsHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	keys := body["keys"].([]any)

	committedOffsets := make(map[string]int)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for _, k := range keys {
		key := k.(string)
		commitIndex, err := commitIndices.ReadInt(ctx, key)
		if err == nil {
			committedOffsets[key] = commitIndex
		}
	}

	replyBody = make(map[string]any)
	replyBody["type"] = "list_committed_offsets_ok"
	replyBody["offsets"] = committedOffsets
	return n.Reply(msg, replyBody)
}
