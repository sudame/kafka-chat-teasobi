package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
	"github.com/sudame/kafka-chat-teasobi/internal/eventtypes"
)

func ListenKafka(onMessagePosted func(eventtypes.PostMessageEventPayload) error) {
	// Kafka consumer
	go func() {
		commaSeparatedBrokers, ok := os.LookupEnv("KAFKA_BROKERS")
		if !ok {
			panic("KAFKA_BROKERS environment variable not set")
		}
		fmt.Printf("Connecting to Kafka brokers: %s\n", commaSeparatedBrokers)

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   strings.Split(commaSeparatedBrokers, ","),
			Topic:     "chat-events",
			Partition: 0,
		})
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				log.Println("read error:", err)
				continue
			}
			var event eventtypes.PostMessageEvent
			if err := json.Unmarshal(m.Value, &event); err != nil {
				log.Println("unmarshal error:", err)
				continue
			}
			if event.Type == eventtypes.PostMessageEventTypeNew && event.Version == 1 {
				if err := onMessagePosted(event.Payload); err != nil {
					log.Println("error handling message:", err)
					continue
				}
			}
		}
	}()
}
