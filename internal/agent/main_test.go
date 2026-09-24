package agent

import (
	"os"
	"testing"
)

// TestMain doubles as the stress child: StartBurn re-executes the test
// binary with ChildEnv set, exactly as the agent re-executes itself.
func TestMain(m *testing.M) {
	if os.Getenv(ChildEnv) == "1" {
		os.Exit(RunStressChild(os.Stdin, os.Stdout))
	}
	os.Exit(m.Run())
}
