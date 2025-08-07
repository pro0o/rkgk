package main

import (
	"encoding/json"
	"maps"
	"os"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type entry struct {
	offset int
	msg    int
}

type server struct {
	node   *maelstrom.Node
	nodeID string
	id     int
	mu     sync.RWMutex

	logs             map[string][]entry
	committedOffsets map[string]int
	latestOffsets    map[string]int
}

type offsetReq struct {
	Offsets map[string]int `json:"offsets"`
}

func init() {
	file, err := os.OpenFile("kafka.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	logger := zerolog.New(file).With().Timestamp().Logger()
	log.Logger = logger
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.nodeID = s.node.ID()
	id, err := strconv.Atoi(s.nodeID[1:])
	if err != nil {
		return err
	}
	s.id = id
	return nil
}

func main() {
	node := maelstrom.NewNode()
	s := &server{
		node:             node,
		logs:             make(map[string][]entry),
		committedOffsets: make(map[string]int),
		latestOffsets:    make(map[string]int),
	}

	node.Handle("init", s.initHandler)
	node.Handle("send", s.sendHandler)
	node.Handle("poll", s.pollHandler)
	node.Handle("commit_offsets", s.commitHandler)
	node.Handle("list_committed_offsets", s.listCommitHandler)

	if err := node.Run(); err != nil {
		log.Fatal().Err(err).Msg("node failed to run")
	}
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	message := int(body["msg"].(float64))

	s.mu.Lock()
	currOffset := s.latestOffsets[key] + 1
	s.logs[key] = append(s.logs[key],
		entry{
			offset: currOffset,
			msg:    message,
		})
	s.latestOffsets[key] = currOffset
	s.mu.Unlock()

	log.Info().
		Str("key", key).
		Int("message", message).
		Int("offset", currOffset).
		Msg("Message appended")

	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": currOffset,
	})
}

func binarySearch(entries []entry, offset int) int {
	l, r := 0, len(entries)
	for l < r {
		mid := l + (r-l)/2
		if entries[mid].offset < offset {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := make(map[string][][2]int)

	s.mu.RLock()
	for key, offset := range req.Offsets {
		entries := s.logs[key]
		start := binarySearch(entries, offset)
		for i := start; i < len(entries); i++ {
			e := entries[i]
			res[key] = append(res[key], [2]int{e.offset, e.msg})
		}
		log.Debug().
			Str("key", key).
			Int("start_offset", offset).
			Int("returned", len(res[key])).
			Msg("Polling entries")
	}
	s.mu.RUnlock()

	return s.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": res,
	})
}

func (s *server) commitHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	maps.Copy(s.committedOffsets, req.Offsets)
	s.mu.Unlock()

	return s.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) listCommitHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	res := make(map[string]int)
	keys := body["keys"].([]any)

	s.mu.RLock()
	for _, key := range keys {
		k := key.(string)
		res[k] = s.committedOffsets[k]
	}
	s.mu.RUnlock()

	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}
