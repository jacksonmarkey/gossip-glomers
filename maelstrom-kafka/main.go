package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Entry = struct {
	offset int
	msg    int
}
type LockedLog = struct {
	commitIndex int
	nextIndex   int
	entries     []Entry
	mu          sync.Mutex
}

var n *maelstrom.Node
var logs map[string]*LockedLog
var createLogLock *sync.Mutex

func main() {
	createLogLock = &sync.Mutex{}
	logs = make(map[string]*LockedLog)
	n = maelstrom.NewNode()
	n.Handle("send", sendHandler)
	n.Handle("poll", pollHandler)
	n.Handle("commit_offsets", commitOffsetsHandler)
	n.Handle("list_committed_offsets", listCommittedOffsetsHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func getOrCreateLog(key string) *LockedLog {
	log, ok := logs[key]
	if !ok {
		createLogLock.Lock()
		log, ok = logs[key]
		if !ok {
			log = &LockedLog{}
			logs[key] = log
		}
		createLogLock.Unlock()
	}
	return log
}

func sendHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	logKey := body["key"].(string)
	logMsg := int(body["msg"].(float64))

	log := getOrCreateLog(logKey)
	log.mu.Lock()
	offset := log.nextIndex
	log.entries = append(log.entries, Entry{offset, logMsg})
	log.nextIndex += 1
	log.mu.Unlock()

	replyBody = make(map[string]any)
	replyBody["type"] = "send_ok"
	replyBody["offset"] = offset
	return n.Reply(msg, replyBody)
}

func pollHandler(msg maelstrom.Message) error {
	var body, replyBody map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	returnMessages := make(map[string][][]int)
	offsets := body["offsets"].(map[string]any)
	for logKey, o := range offsets {
		logMessagesPastOffset := make([][]int, 0)
		lockedLog, ok := logs[logKey]
		if ok {
			lockedLog.mu.Lock()
			logOffset := int(o.(float64))
			for i, e := range lockedLog.entries {
				if e.offset >= logOffset {
					logMessagesPastOffset = make([][]int, len(lockedLog.entries)-i)
					for j, e := range lockedLog.entries[i:] {
						logMessagesPastOffset[j] = []int{e.offset, e.msg}
					}
					break
				}
			}
			lockedLog.mu.Unlock()
		}
		returnMessages[logKey] = logMessagesPastOffset
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
	for logKey, o := range offsets {
		logOffset := int(o.(float64))
		lockedLog := getOrCreateLog(logKey)
		lockedLog.mu.Lock()
		if logOffset > lockedLog.commitIndex {
			lockedLog.commitIndex = logOffset
		}
		lockedLog.mu.Unlock()
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
	for _, k := range keys {
		key := k.(string)
		log, ok := logs[key]
		if ok {
			log.mu.Lock()
			offset := log.commitIndex
			committedOffsets[key] = offset
			log.mu.Unlock()
		}
	}

	replyBody = make(map[string]any)
	replyBody["type"] = "list_committed_offsets_ok"
	replyBody["offsets"] = committedOffsets
	return n.Reply(msg, replyBody)
}
