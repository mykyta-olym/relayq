//go:build failure && race

package failure

// raceEnabled builds the worker binary with the race detector too, so the killed
// processes are checked as well as the test process.
const raceEnabled = true
