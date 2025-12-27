package protocol

const (
	ActionSubscribe      = "subscribe"
	ActionUnsubscribe    = "unsubscribe"
	ActionUnsubscribeAll = "unsubscribe_all"
)

type WSRequest struct {
	Action  string         `json:"action"`
	Payload RequestPayload `json:"payload"`
	ID      string         `json:"id,omitempty"`
}

type RequestPayload struct {
	Symbols []string `json:"symbols"`
}

type WSResponse struct {
	Type    string      `json:"type"`             // "ack", "error", "ticker"
	ID      string      `json:"id,omitempty"`     // Matches request ID
	Status  string      `json:"status,omitempty"` // "success", "error"
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}
