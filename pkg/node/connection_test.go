package node

import (
	"testing"
)

func TestConnection_ValidTransitions(t *testing.T) {
	cases := []struct {
		from connectionState
		to   connectionState
	}{
		{stateConnecting, stateConnected},
		{stateConnected, stateDisconnecting},
		{stateDisconnecting, stateDisconnected},
	}
	for _, tc := range cases {
		conn := &connection{connState: tc.from}
		if err := conn.transition(tc.to); err != nil {
			t.Errorf("%s → %s: unexpected error: %v", tc.from, tc.to, err)
		}
		if conn.state() != tc.to {
			t.Errorf("state not updated: got %s, want %s", conn.state(), tc.to)
		}
	}
}

func TestConnection_InvalidTransitions(t *testing.T) {
	cases := []struct {
		from connectionState
		to   connectionState
	}{
		{stateDisconnected, stateConnected},
		{stateDisconnected, stateConnecting},
		{stateConnected, stateConnecting},
		{stateConnected, stateDisconnected},
		{stateDisconnecting, stateConnected},
		{stateConnecting, stateDisconnected},
	}
	for _, tc := range cases {
		conn := &connection{connState: tc.from}
		if err := conn.transition(tc.to); err == nil {
			t.Errorf("%s → %s: expected error, got nil", tc.from, tc.to)
		}
	}
}

func TestConnection_StateString(t *testing.T) {
	cases := map[connectionState]string{
		stateConnecting:    "CONNECTING",
		stateConnected:     "CONNECTED",
		stateDisconnecting: "DISCONNECTING",
		stateDisconnected:  "DISCONNECTED",
	}
	for state, want := range cases {
		if got := state.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
