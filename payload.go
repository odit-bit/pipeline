package pipeline

type Payload interface {
	MarkAsProcessed()
	Clone() Payload
}

