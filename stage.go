package pipeline

import (
	"context"
	"sync"
)

var _ Stage = (*fifo)(nil)

type fifo struct {
	proc Processor
}

func NewFifo(proc Processor) *fifo {
	f := fifo{
		proc: proc,
	}
	return &f
}

// Run implements Stage.
// Run running the stage process. it ready for concurrent process
func (f *fifo) Run(ctx context.Context, in <-chan Payload, errCh chan<- error, out chan<- Payload) {
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-in:
			if !ok {
				return
			}

			newP, err := f.proc.Process(ctx, p)
			if err != nil {
				errCh <- err
				continue
			}

			select {
			case <-ctx.Done():
				return
			case out <- newP:
				// newP add to (downstream) out chan
			}
		}
	}

}

var _ Stage = (*muxStage)(nil)

// stage that multiplexing process by number of worker
type muxStage struct {
	stage []Stage
}

// create stage instance  that multiplexing process by number of worker
func NewMuxStage(num int, proc Processor) *muxStage {
	fifos := make([]Stage, num)

	for i := 0; i < num; i++ {
		fifos[i] = NewFifo(proc)
	}

	ms := muxStage{
		stage: fifos,
	}

	return &ms
}

// Run implements Stage.
func (fo *muxStage) Run(ctx context.Context, in <-chan Payload, errC chan<- error, out chan<- Payload) {

	// muxInChan := make(chan Payload)
	var wg sync.WaitGroup

	for _, f := range fo.stage {
		wg.Add(1)
		go func(ctx context.Context, stage Stage) {
			defer wg.Done()
			stage.Run(ctx, in, errC, out)
		}(ctx, f)
	}

	wg.Wait()
}

type broadcast struct {
	stages []Stage
}

func NewBroadcast(procs ...Processor) *broadcast {
	if len(procs) == 0 {
		panic("Broadcast: at least one processor must be specified")
	}

	fifos := make([]Stage, len(procs))
	for i, p := range procs {
		fifos[i] = NewFifo(p)
	}

	return &broadcast{stages: fifos}
}

func (bc *broadcast) Run(ctx context.Context, in <-chan Payload, errC chan<- error, out chan<- Payload) {
	var (
		wg   sync.WaitGroup
		inCh = make([]chan Payload, len(bc.stages))
	)

	// Start each FIFO in a go-routine. Each FIFO gets its own dedicated
	// input channel and the shared output channel passed to Run.
	for i := 0; i < len(bc.stages); i++ {
		wg.Add(1)
		inCh[i] = make(chan Payload)
		go func(stageIndex int) {
			bc.stages[stageIndex].Run(ctx, inCh[stageIndex], errC, out)
			wg.Done()
		}(i)
	}

done:
	for {
		// Read incoming payloads and pass them to each FIFO
		select {
		case <-ctx.Done():
			break done
		case payload, ok := <-in:
			if !ok {
				break done
			}
			for i := len(bc.stages) - 1; i >= 0; i-- {
				// As each FIFO might modify the payload, to
				// avoid data races we need to make a copy of
				// the payload for all FIFOs except the first.
				var fifoPayload = payload
				if i != 0 {
					fifoPayload = payload.Clone()
				}
				select {
				case <-ctx.Done():
					break done
				case inCh[i] <- fifoPayload:
					// payload sent to i_th FIFO
				}
			}
		}
	}

	// Close input channels and wait for FIFOs to exit
	for _, ch := range inCh {
		close(ch)
	}
	wg.Wait()
}
