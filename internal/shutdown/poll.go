package shutdown

import "time"

// pollAfter returns a channel that fires after a short interval. Extracted as
// a function so tests could shrink it; production uses 5ms (negligible CPU,
// fast drain).
func pollAfter() <-chan time.Time { return time.After(5 * time.Millisecond) }
