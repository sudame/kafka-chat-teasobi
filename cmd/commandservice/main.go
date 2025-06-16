package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sudame/kafka-chat-teasobi/internal/eventtypes"
)

func main() {
	commaSeparatedBrokers, ok := os.LookupEnv("KAFKA_BROKERS")
	if !ok {
		panic("KAFKA_BROKERS environment variable not set")
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(commaSeparatedBrokers, ","),
		Topic:   "chat-events",
	})

	http.HandleFunc("/chat", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Username string `json:"username"`
			Message  string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			log.Printf("body: %s", r.Body)
			log.Printf("failed to decode request body: %v", err)
			return
		}
		event := eventtypes.PostMessageEvent{
			Type:    eventtypes.PostMessageEventTypeNew,
			Version: 1,
			Payload: eventtypes.PostMessageEventPayload{
				Username:  input.Username,
				Message:   input.Message,
				Timestamp: time.Now().UnixMilli(),
			},
		}
		value, _ := json.Marshal(event)
		if err := writer.WriteMessages(context.Background(), kafka.Message{Value: value}); err != nil {
			log.Printf("failed to write message: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	})

	log.Println("Command service listening on :3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		log.Fatal(err)
	}
}
