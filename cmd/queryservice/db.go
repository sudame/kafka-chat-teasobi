package main

import (
	"database/sql"

	"github.com/sudame/kafka-chat-teasobi/internal/eventtypes"
)

func InitDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./db/chat_read.db")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		username TEXT,
		message TEXT,
		timestamp INTEGER
	)`); err != nil {
		return nil, err
	}

	return db, nil
}

func GetAllMessages(db *sql.DB) ([]eventtypes.PostMessageEventPayload, error) {
	rows, err := db.Query("SELECT username, message, timestamp FROM messages ORDER BY timestamp ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []eventtypes.PostMessageEventPayload
	for rows.Next() {
		var msg eventtypes.PostMessageEventPayload
		if err := rows.Scan(&msg.Username, &msg.Message, &msg.Timestamp); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}

	return messages, nil
}
