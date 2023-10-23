package pipeline

import (
	"context"
	"fmt"
	"log"
	"sync"
)

//this is user facing API

type Stage interface {
	Run(ctx context.Context, in <-chan Payload, errC chan<- error, out chan<- Payload)
}

type pipe struct {
	stages  []Stage
	errFunc ErrorFunc
}

type Config struct {
	OnError ErrorFunc
}

func (conf *Config) validate() {
	if conf.OnError == nil {
		conf.OnError = func(err error) {
			log.Println("[pipe] ", err)
		}
	}
}

func NewPipe(conf *Config, stages ...Stage) *pipe {

	p := pipe{
		stages: stages,
	}

	conf.validate()
	p.OnError(conf.OnError)

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

func (p *pipe) onError(err error) {
	if p.errFunc != nil {
		p.errFunc(err)
	}
}

type ErrorFunc func(err error)

func (p *pipe) OnError(fn ErrorFunc) {
	p.errFunc = fn
}

// ============================================================

type Source interface {
	Next() bool
	Value() Payload
	Error() error
}
type Destination interface {
	Consume(p Payload) error
}

func (p *pipe) Run(ctx context.Context, src Source, dst Destination) error {
	// in := make(chan Payload)
	// out := make(chan Payload)
	errC := make(chan error)
	pCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup

	// create chan for every stage
	stagesCh := make([]chan Payload, len(p.stages)+1)
	for i := range stagesCh {
		stagesCh[i] = make(chan Payload)
	}

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
	for err = range errC {
		p.onError(err)
		cancel()
	}

	return err
}

func sourceFunc(ctx context.Context, src Source, in chan<- Payload, errC chan<- error) {

	for src.Next() {
		p := src.Value()
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
			if err := dst.Consume(v); err != nil {
				errC <- err
				return
			}
			v.MarkAsProcessed()
		}
	}

}
