package pipeline

import "context"

// stage processor that process pipeline payload
type Processor interface {
	Process(ctx context.Context, payload Payload) (Payload, error)
}

type ProcessorFunc func(ctx context.Context, payload Payload) (Payload, error)

func (pf ProcessorFunc) Process(ctx context.Context, payload Payload) (Payload, error) {
	return pf(ctx, payload)
}
