package eventtypes

type PostMessageEventType string

const (
	PostMessageEventTypeNew PostMessageEventType = "post"
)

type PostMessageEventPayload struct {
	Username  string `json:"username"`
	Message   string `json:"message"`
	Timestamp int64  `json:"timestamp"`
}

type PostMessageEvent struct {
	Type    PostMessageEventType    `json:"type"`
	Version int                     `json:"version"`
	Payload PostMessageEventPayload `json:"payload"`
}
