package pipeline

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

var errTest = fmt.Errorf("test error")

func Test_Fifo_Run(t *testing.T) {
	var tt = []struct {
		payload         Payload
		proc            Processor
		testErr         error
		expectedPayload Payload
	}{
		{
			payload: &mockPayload{value: "pass"},

			proc: func() ProcessorFunc {
				return func(ctx context.Context, payload Payload) (Payload, error) {
					mock, _ := payload.(*mockPayload)
					if mock.value == "pass" {
						return mock, nil
					}
					return nil, errTest
				}
			}(),

			testErr:         nil,
			expectedPayload: &mockPayload{value: "pass"},
		},
		{
			payload: &mockPayload{value: ""},
			proc: func() ProcessorFunc {
				return func(ctx context.Context, payload Payload) (Payload, error) {
					mock, _ := payload.(*mockPayload)
					if mock.value == "pass" {
						return mock, nil
					}
					return nil, errTest
				}
			}(),

			testErr:         errTest,
			expectedPayload: nil,
		},
	}

	for _, tc := range tt {
		f := NewFifo(tc.proc)
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		src := make(chan Payload)
		out := make(chan Payload)
		errCh := make(chan error)

		var p Payload
		var err error
		t.Run("", func(t *testing.T) {
			go f.Run(ctx, src, errCh, out)

			go func() {
				src <- tc.payload
				close(src)
			}()

			select {
			case <-ctx.Done():
			case p = <-out:
			case err = <-errCh:
			}

			close(out)
			close(errCh)

			if ctx.Err() != nil {
				t.Fatal(ctx.Err())
			}
		})

		if !reflect.DeepEqual(p, tc.expectedPayload) {
			t.Fatalf("\nexpected:%v, got:%v \n", tc.expectedPayload, p)
		}

		if !errors.Is(err, tc.testErr) {
			t.Fatalf("\nexpected:%v, got:%v \n", tc.testErr, err)
		}
	}
}

func Test_mux_stage(t *testing.T) {
	proc := func() ProcessorFunc {
		return func(ctx context.Context, payload Payload) (Payload, error) {
			mock, _ := payload.(*mockPayload)
			if mock.value == "pass" {
				return mock, nil
			}
			return nil, errTest
		}
	}()

	ctx := context.TODO()
	src := make(chan Payload)
	out := make(chan Payload)
	errCh := make(chan error)

	mp := mockPayload{
		value: "pass",
	}
	expectedPayload := &mp
	var testErr error

	mux := NewMuxStage(3, proc)
	go mux.Run(ctx, src, errCh, out)

	src <- &mp
	close(src)

	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())

	case p := <-out:
		if !reflect.DeepEqual(p, expectedPayload) {
			t.Fatalf("\nexpected:%v, got:%v \n", expectedPayload, p)
		}

	case err := <-errCh:
		if !errors.Is(err, testErr) {
			t.Fatalf("\nexpected:%v, got:%v \n", testErr, err)
		}
	}

}
