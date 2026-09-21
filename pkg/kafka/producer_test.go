package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

func TestIsUnknownTopic(t *testing.T) {
	other := errors.New("broker on fire")
	cases := map[string]struct {
		err  error
		want bool
	}{
		"the bare protocol error":           {kafkago.UnknownTopicOrPartition, true},
		"wrapped protocol error":            {fmt.Errorf("write: %w", kafkago.UnknownTopicOrPartition), true},
		"every message unknown topic":       {kafkago.WriteErrors{kafkago.UnknownTopicOrPartition, kafkago.UnknownTopicOrPartition}, true},
		"unknown topic beside success":      {kafkago.WriteErrors{nil, kafkago.UnknownTopicOrPartition}, true},
		"unknown topic beside a real error": {kafkago.WriteErrors{kafkago.UnknownTopicOrPartition, other}, false},
		"some other error":                  {other, false},
		"another protocol error":            {kafkago.NotLeaderForPartition, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isUnknownTopic(tc.err); got != tc.want {
				t.Errorf("isUnknownTopic(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRetryUnknownTopic_SucceedsOnceTheTopicIsKnown(t *testing.T) {
	calls := 0
	err := retryUnknownTopic(context.Background(), 5, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return kafkago.UnknownTopicOrPartition
		}
		return nil
	})

	if err != nil || calls != 3 {
		t.Errorf("got err=%v after %d calls, want nil after 3", err, calls)
	}
}

func TestRetryUnknownTopic_OtherErrorsAreNotRetried(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	err := retryUnknownTopic(context.Background(), 5, time.Millisecond, func() error {
		calls++
		return boom
	})

	if !errors.Is(err, boom) || calls != 1 {
		t.Errorf("got err=%v after %d calls, want boom after exactly 1", err, calls)
	}
}

func TestRetryUnknownTopic_GivesUpAfterTheAttemptLimit(t *testing.T) {
	calls := 0
	err := retryUnknownTopic(context.Background(), 4, time.Millisecond, func() error {
		calls++
		return kafkago.UnknownTopicOrPartition
	})

	if !isUnknownTopic(err) || calls != 4 {
		t.Errorf("got err=%v after %d calls, want the unknown-topic error after exactly 4", err, calls)
	}
}

func TestRetryUnknownTopic_StopsWaitingWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := retryUnknownTopic(ctx, 5, time.Hour, func() error {
		calls++
		cancel() // shutdown arrives while we would be sleeping between attempts
		return kafkago.UnknownTopicOrPartition
	})

	if !isUnknownTopic(err) || calls != 1 {
		t.Errorf("got err=%v after %d calls, want to return promptly after the first attempt", err, calls)
	}
}
