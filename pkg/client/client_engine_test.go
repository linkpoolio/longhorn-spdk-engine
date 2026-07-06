package client

import (
	"strings"
	"testing"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

// EngineCreate must reject requests whose replica set cannot be honored:
// Engine.Create iterates replicaAddressMap only, so a transport-map-only
// request (or a transport-map key without an address-map entry) would
// silently produce an engine missing those replicas.
func TestEngineCreateRejectsTransportMapOnlyRequests(t *testing.T) {
	c := &SPDKClient{}

	// Transport-map-only: no replicaAddressMap entries at all.
	_, err := c.EngineCreate("e-1", "vol-1", "spdk-tcp-nvmf", 1024,
		nil,
		map[string]*spdkrpc.ReplicaTransportAddresses{
			"r-1": {TcpAddress: "10.0.0.1:20001"},
		},
		1, false, 0)
	if err == nil {
		t.Fatal("expected error for transport-map-only request, got nil")
	}
	if !strings.Contains(err.Error(), "replicaAddressMap") {
		t.Fatalf("unexpected error: %v", err)
	}

	// Transport-map key without a matching replicaAddressMap entry.
	_, err = c.EngineCreate("e-1", "vol-1", "spdk-tcp-nvmf", 1024,
		map[string]string{"r-1": "10.0.0.1:20001"},
		map[string]*spdkrpc.ReplicaTransportAddresses{
			"r-1": {TcpAddress: "10.0.0.1:20001"},
			"r-2": {TcpAddress: "10.0.0.2:20001"},
		},
		1, false, 0)
	if err == nil {
		t.Fatal("expected error for transport-map key missing from replicaAddressMap, got nil")
	}
	if !strings.Contains(err.Error(), "r-2") {
		t.Fatalf("expected the orphan replica name in the error, got: %v", err)
	}
}
