package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type server struct {
	node   *maelstrom.Node
	kv     *maelstrom.KV
	nodeID string
	id     int
}

type offsetReq struct {
	Offsets map[string]int `json:"offsets"`
}

const (
	prefixEntry  = "prefix__"
	prefixLatest = "latest__"
	prefixCommit = "commit__"
)

const MAX_RETRIES = 10

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
	log.Info().Str("node", s.nodeID).Msg("Node initialized")
	return nil
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)

	s := &server{
		node: node,
		kv:   kv,
	}

	node.Handle("init", s.initHandler)
	node.Handle("send", s.sendHandler)
	node.Handle("poll", s.pollHandler)
	node.Handle("commit_offsets", s.commitHandler)
	node.Handle("list_committed_offsets", s.listCommitHandler)

	if err := node.Run(); err != nil {
		return
	}
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	message := int(body["msg"].(float64))

	keyLatest := fmt.Sprintf("%s%s", prefixLatest, key)
	offset, err := s.kv.ReadInt(context.Background(), keyLatest)
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			offset = 0
		} else {
			log.Error().Err(err).Str("key", key).Msg("Failed to read latest offset")
			return err
		}
	} else {
		offset++
	}

	for ; ; offset++ {
		err := s.kv.CompareAndSwap(context.Background(), keyLatest, offset-1, offset, true)
		if err != nil {
			log.Warn().Err(err).Int("try_offset", offset).Msg("CAS failed, retrying")
			continue
		}
		log.Info().Str("key", key).Int("offset", offset).Msg("CAS success, proceeding to write message")
		break
	}

	entryKey := fmt.Sprintf("%s%s_%d", prefixEntry, key, offset)
	if err := s.kv.Write(context.Background(), entryKey, message); err != nil {
		log.Error().Err(err).Str("entryKey", entryKey).Msg("Failed to write message to KV")
		return err
	}

	log.Info().Str("key", key).Int("offset", offset).Int("message", message).Msg("Message sent successfully")
	return s.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (s *server) getValue(key string, offset int) (int, bool, error) {
	val, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%s%s_%d", prefixEntry, key, offset))
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	return val, true, nil
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	log.Info().Interface("offsets", req.Offsets).Msg("Polling started")

	res := make(map[string][][2]int)

	for key, startingOffset := range req.Offsets {
		for offset := startingOffset; ; offset++ {
			message, exists, err := s.getValue(key, offset)
			if err != nil {
				log.Error().Err(err).Str("key", key).Int("offset", offset).Msg("Failed to read message during poll")
				return err
			}
			if !exists {
				break
			}
			res[key] = append(res[key], [2]int{offset, message})
		}
	}

	log.Info().Interface("result", res).Msg("Polling completed")
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

	for key, offset := range req.Offsets {
		if err := s.kv.Write(context.Background(), fmt.Sprintf("%s%s", prefixCommit, key), offset); err != nil {
			log.Error().Err(err).Str("key", key).Int("offset", offset).Msg("Commit write failed")
			return err
		}
		log.Info().Str("key", key).Int("offset", offset).Msg("Offset committed")
	}

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

	for _, key := range keys {
		k := key.(string)
		offset, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%s%s", prefixCommit, k))
		if err != nil {
			offset = 0
		}
		res[k] = offset
	}

	log.Info().Interface("offsets", res).Msg("Committed offsets returned")
	return s.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}
