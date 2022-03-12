package main

type LamportClock struct {
	LatestTime int64
}

// NewLamportClock returns a new instance of LamportClock with time set to 0.
func NewLamportClock() LamportClock {
	return LamportClock{LatestTime: 0}
}

// Tick increments the latest time
func (lc *LamportClock) Tick(requestTime int64) int64 {
	lc.LatestTime = maxTime(lc.LatestTime, requestTime) + 1
	return lc.LatestTime
}

// maxTime returns the max of the two given times.
func maxTime(t1, t2 int64) int64 {
	if t1 < t2 {
		return t2
	}
	return t1
}
