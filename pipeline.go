package pipeline

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/multierr"
)

//this is user facing API

type Stage interface {
	Run(ctx context.Context, in <-chan Payload, errC chan<- error, out chan<- Payload)
}

type Pipe struct {
	stages []Stage
}

type Config struct {
}

func (conf *Config) validate() {

}

func NewPipe(conf *Config, stages ...Stage) *Pipe {

	p := Pipe{
		stages: stages,
	}

	conf.validate()

	return &p

}

// func (p *pipe) Run(ctx context.Context, src <-chan Payload, dst chan<- Payload) error {
// 	var wg sync.WaitGroup
// 	pCtx, cancel := context.WithCancel(ctx)
// 	errC := make(chan error)

// 	// create chan for every stage
// 	stagesCh := make([]chan Payload, len(p.stages)+1)
// 	for i := range stagesCh {
// 		stagesCh[i] = make(chan Payload)
// 	}

// 	// assign stageCh into stages
// 	for i, v := range p.stages {
// 		wg.Add(1)
// 		go func(i int, stageCtx context.Context, stage Stage) {
// 			stage.Run(stageCtx, stagesCh[i], errC, stagesCh[i+1])
// 			close(stagesCh[i+1])
// 			wg.Done()
// 		}(i, pCtx, v)

// 	}

// 	// src
// 	// if src chan is close it should close the first stage input chan
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		defer close(stagesCh[0])

// 		for p := range src {
// 			select {
// 			case <-pCtx.Done():
// 				return
// 			case stagesCh[0] <- p:
// 			}
// 		}
// 	}()

// 	// dst
// 	// if not other payload comin from last stage out chan, the for loop will exit
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for v := range stagesCh[len(stagesCh)-1] {
// 			select {
// 			case <-pCtx.Done():
// 				return
// 			case dst <- v:
// 			}
// 		}

// 	}()

// 	// should wait before all goroutine is done,
// 	// and close the errC
// 	go func() {
// 		wg.Wait()
// 		close(errC)
// 		cancel()
// 	}()

// 	var err error
// 	for err = range errC {
// 		p.onError(err)
// 		cancel()
// 	}

// 	return err
// }

// ============================================================

type Source interface {
	Next() bool
	Payload() Payload
	Error() error
}
type Destination interface {
	Consume(ctx context.Context, p Payload) error
}

func (p *Pipe) Run(ctx context.Context, src Source, dst Destination) error {
	// in := make(chan Payload)
	// out := make(chan Payload)

	// create chan for every stage
	stagesCh := make([]chan Payload, len(p.stages)+1)
	errC := make(chan error, len(p.stages)+2)
	pCtx, cancel := context.WithCancel(ctx)

	for i := range stagesCh {
		stagesCh[i] = make(chan Payload)
	}

	var wg sync.WaitGroup
	// assign stageCh into stages
	for i, v := range p.stages {
		wg.Add(1)
		go func(i int, stageCtx context.Context, stage Stage) {
			stage.Run(stageCtx, stagesCh[i], errC, stagesCh[i+1])
			close(stagesCh[i+1])
			wg.Done()
		}(i, pCtx, v)

	}

	//src
	wg.Add(1)
	go func() {
		defer wg.Done()
		sourceFunc(ctx, src, stagesCh[0], errC)
		close(stagesCh[0])
	}()

	// dest
	wg.Add(1)
	go func() {
		defer wg.Done()
		destFunc(ctx, dst, stagesCh[len(stagesCh)-1], errC)
	}()

	// should wait before all goroutine is done,
	// and close the errC
	go func() {
		wg.Wait()
		close(errC)
		cancel()
	}()

	var err error
	for newErr := range errC {
		//TODO: way of emitting error
		err = multierr.Append(err, newErr)
		cancel()
	}

	return err
}

func sourceFunc(ctx context.Context, src Source, in chan<- Payload, errC chan<- error) {

	for src.Next() {
		p := src.Payload()
		select {
		case <-ctx.Done():
			return
		case in <- p:
		}
	}

	if err := src.Error(); err != nil {
		errC <- fmt.Errorf("pipeline source err: %v ", err)
	}

}

func destFunc(ctx context.Context, dst Destination, out <-chan Payload, errC chan<- error) {

	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-out:
			if !ok {
				return
			}
			if err := dst.Consume(ctx, v); err != nil {
				errC <- err
				return
			}
			v.MarkAsProcessed()
		}
	}

}
