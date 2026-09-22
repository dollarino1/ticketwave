package server

import "testing"

func TestClampLimit(t *testing.T) {
	cases := map[int32]int32{
		-5:   defaultLimit,
		0:    defaultLimit,
		1:    1,
		50:   50,
		100:  100,
		101:  maxLimit,
		9999: maxLimit,
	}
	for in, want := range cases {
		if got := clampLimit(in); got != want {
			t.Errorf("clampLimit(%d) = %d, want %d", in, got, want)
		}
	}
}
