package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sudame/kafka-chat-teasobi/internal/eventtypes"
)

var (
	clients   = make(map[chan eventtypes.PostMessageEventPayload]struct{})
	clientsMu sync.Mutex
)

func broadcast(msg eventtypes.PostMessageEventPayload) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	for ch := range clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func main() {
	db, err := InitDb()
	if err != nil {
		log.Fatal("failed to initialize database:", err)
	}

	// Kafka consumer
	onMessagePosted := func(event eventtypes.PostMessageEventPayload) error {
		if _, err := db.Exec(`INSERT INTO messages (username, message, timestamp) VALUES (?, ?, ?)`,
			event.Username, event.Message, event.Timestamp); err != nil {
			return err
		}
		broadcast(event)
		return nil
	}
	ListenKafka(onMessagePosted)

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
			return
		}

		messages, err := GetAllMessages(db)
		if err != nil {
			log.Println("error getting messages:", err)
			http.Error(w, "Failed to get messages", http.StatusInternalServerError)
			return
		}
		for _, msg := range messages {
			fmt.Fprintf(w, "data: %s\n\n", toJSON(msg))
			flusher.Flush()
		}

		msgCh := make(chan eventtypes.PostMessageEventPayload, 10)
		clientsMu.Lock()
		clients[msgCh] = struct{}{}
		clientsMu.Unlock()

		defer func() {
			clientsMu.Lock()
			delete(clients, msgCh)
			clientsMu.Unlock()
			close(msgCh)
		}()

		for msg := range msgCh {
			fmt.Fprintf(w, "data: %s\n\n", toJSON(msg))
			flusher.Flush()
		}
	})

	log.Println("Query service listening on :3001")
	if err := http.ListenAndServe(":3001", nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
