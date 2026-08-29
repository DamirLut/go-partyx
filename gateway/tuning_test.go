package gateway

import (
	"testing"
	"time"
)

func TestTuningDefaults(t *testing.T) {
	got := newTuning(Config{})
	if got.writeWait != defaultWriteWait ||
		got.pongWait != defaultPongWait ||
		got.pingPeriod != defaultPingPeriod ||
		got.authTimeout != defaultAuthTimeout ||
		got.maxMessageSize != defaultMaxMessageSize ||
		got.sendBufferSize != defaultSendBufferSize ||
		got.shutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("tuning = %+v, want all defaults", got)
	}
}

func TestTuningAppliesOnlyZeroFields(t *testing.T) {
	got := newTuning(Config{
		MaxMessageSize: 1024,
		AuthTimeout:    3 * time.Second,
		PongWait:       10 * time.Second,
		SendBufferSize: 8,
	})
	if got.maxMessageSize != 1024 || got.authTimeout != 3*time.Second ||
		got.pongWait != 10*time.Second || got.sendBufferSize != 8 {
		t.Fatalf("tuning = %+v, want configured values kept", got)
	}
	// Zero fields still get defaults.
	if got.writeWait != defaultWriteWait || got.shutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("tuning = %+v, want defaults for zero fields", got)
	}
	// A ping period that would not beat the pong wait falls back to default.
	if got.pingPeriod != defaultPingPeriod {
		t.Fatalf("pingPeriod = %v, want default (%v must stay below pongWait)", got.pingPeriod, 10*time.Second)
	}
}
