package pipeline

import (
	"context"
	"testing"
)

// func Test_Pipe(t *testing.T) {
// 	proc := func() ProcessorFunc {
// 		return func(ctx context.Context, payload Payload) (Payload, error) {
// 			mock, _ := payload.(*mockProc)
// 			if mock.val == "pass" {
// 				return mock, nil
// 			}
// 			return nil, errTest
// 		}
// 	}()
// 	stage1 := NewFifo(proc)
// 	stage2 := NewMuxStage(2, proc)

// 	pipe := NewPipe(&Config{}, stage1, stage2)

// 	src := make(chan Payload)
// 	dst := make(chan Payload)

// 	testPayload := &mockProc{val: "pass"}
// 	go func() {
// 		src <- testPayload
// 		close(src)
// 	}()

// 	var result Payload
// 	var ok bool
// 	go func() {
// 		result, ok = <-dst
// 		close(dst)
// 	}()

// 	err := pipe.Run(context.Background(), src, dst)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	if !ok {
// 		t.Fatal("dst chan is closed")
// 	}

// 	if !reflect.DeepEqual(result, testPayload) {
// 		t.Fatal(result)
// 	}
// }

// func Test_Pipe_error(t *testing.T) {
// 	proc := func() ProcessorFunc {
// 		return func(ctx context.Context, payload Payload) (Payload, error) {
// 			mock, _ := payload.(*mockProc)
// 			if mock.val == "pass" {
// 				return mock, nil
// 			}
// 			return nil, errTest
// 		}
// 	}()
// 	stage1 := NewFifo(proc)
// 	stage2 := NewMuxStage(2, proc)

// 	pipe := NewPipe(&Config{}, stage1, stage2)

// 	src := make(chan Payload)
// 	dst := make(chan Payload)

// 	testPayload := &mockProc{val: "not passed"}
// 	go func() {
// 		src <- testPayload
// 		close(src)
// 	}()

// 	var result Payload
// 	var ok bool
// 	go func() {
// 		result, ok = <-dst
// 		close(dst)
// 	}()

// 	err := pipe.Run(context.Background(), src, dst)

// 	if !errors.Is(err, errTest) {
// 		t.Fatal(err)
// 	}

// 	if ok {
// 		t.Fatal("dst chan should closed")
// 	}

// 	if !reflect.DeepEqual(result, nil) {
// 		t.Fatal(result)
// 	}
// }

var _ Payload = (*mockPayload)(nil)

type mockPayload struct {
	value string
}

// Clone implements Payload.
func (mp *mockPayload) Clone() Payload {
	newP := new(mockPayload)
	newP.value = mp.value
	return newP
}

// MarkAsProcessed implements Payload.
func (*mockPayload) MarkAsProcessed() {

}

var _ Source = (*mockSource)(nil)

type mockSource struct {
	idx int
	src []string
}

// Next implements Source.
func (ms *mockSource) Next() bool {
	return ms.idx < len(ms.src)
}

// Value implements Source.
func (ms *mockSource) Payload() Payload {
	v := ms.src[ms.idx]
	p := mockPayload{
		value: v,
	}
	ms.idx++
	return &p
}

func (ms *mockSource) Error() error {
	return nil
}

var _ Destination = (*mockDest)(nil)

type mockDest struct{}

// Consume implements Destination.
func (*mockDest) Consume(ctx context.Context, p Payload) error {
	return nil
}

func Test_pipe(t *testing.T) {
	proc := func() ProcessorFunc {
		return func(ctx context.Context, payload Payload) (Payload, error) {
			mock, _ := payload.(*mockPayload)
			if mock.value == "pass" {
				return mock, nil
			}
			return nil, errTest
		}
	}()
	p := NewPipe(&Config{}, NewFifo(proc), NewMuxStage(3, proc))

	src := mockSource{
		idx: 0,
		src: []string{"pass", "pass", "pass"},
	}
	dst := mockDest{}

	err := p.Run(context.TODO(), &src, &dst)
	if err != nil {
		t.Fatal(err)
	}
}
