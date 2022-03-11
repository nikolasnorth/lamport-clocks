package main

type Timestamp int

type LamportClock struct {
	LatestTime Timestamp
}

// NewLamportClock returns a new instance of LamportClock with time set to 0.
func NewLamportClock() LamportClock {
	return LamportClock{LatestTime: 0}
}

func (lc *LamportClock) Tick(requestTime Timestamp) Timestamp {
	lc.LatestTime = maxTime(lc.LatestTime, requestTime) + 1
	return lc.LatestTime
}

// maxTime returns the max of the two given times.
func maxTime(t1, t2 Timestamp) Timestamp {
	if t1 < t2 {
		return t2
	}
	return t1
}
