package triton

import (
	"testing"
	"time"
)

func TestListDatesFromRange(t *testing.T) {
	start := time.Date(2015, 7, 1, 0, 0, 0, 0, time.UTC)
	dates := listDatesFromRange(start, start)
	if len(dates) != 1 {
		t.Errorf("Not enough dates")
	}

	start = time.Date(2015, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2015, 7, 3, 0, 0, 0, 0, time.UTC)
	dates = listDatesFromRange(start, end)
	if len(dates) != 3 {
		t.Errorf("Not enough dates")
	}
}
