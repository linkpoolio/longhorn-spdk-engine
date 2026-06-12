package spdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/sirupsen/logrus"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/log"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"

	. "gopkg.in/check.v1"
)

// fakeSpdk is a minimal JSON-RPC server speaking just enough of the SPDK
// protocol for the NVMe-oF expose paths, recording the listeners that the
// code under test actually creates. It lets the listener set be compared
// against what the replica advertises — the exact mismatch the 1.12 rebase
// shipped (TCP-only listeners with dual-listener advertisement).
type fakeSpdk struct {
	sync.Mutex
	subsystems []string
	listeners  []fakeListener
	unknown    []string
}

type fakeListener struct {
	Nqn     string
	Trtype  string
	Traddr  string
	Trsvcid string
}

type fakeRPCRequest struct {
	ID     uint32          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func (f *fakeSpdk) serve(conn net.Conn) {
	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)
	for {
		var req fakeRPCRequest
		if err := dec.Decode(&req); err != nil {
			return
		}
		var result interface{}
		switch req.Method {
		case "nvmf_get_transports":
			// Both transports pre-registered so ensureNvmfTransport never
			// needs nvmf_create_transport.
			result = []map[string]string{{"trtype": "RDMA"}, {"trtype": "TCP"}}
		case "nvmf_create_subsystem":
			var p struct {
				Nqn string `json:"nqn"`
			}
			_ = json.Unmarshal(req.Params, &p)
			f.Lock()
			f.subsystems = append(f.subsystems, p.Nqn)
			f.Unlock()
			result = true
		case "nvmf_subsystem_add_ns":
			result = 1
		case "nvmf_subsystem_add_listener":
			var p struct {
				Nqn           string `json:"nqn"`
				ListenAddress struct {
					Trtype  string `json:"trtype"`
					Traddr  string `json:"traddr"`
					Trsvcid string `json:"trsvcid"`
				} `json:"listen_address"`
			}
			_ = json.Unmarshal(req.Params, &p)
			f.Lock()
			f.listeners = append(f.listeners, fakeListener{
				Nqn:     p.Nqn,
				Trtype:  p.ListenAddress.Trtype,
				Traddr:  p.ListenAddress.Traddr,
				Trsvcid: p.ListenAddress.Trsvcid,
			})
			f.Unlock()
			result = true
		default:
			f.Lock()
			f.unknown = append(f.unknown, req.Method)
			f.Unlock()
			result = true
		}
		resp := map[string]interface{}{"id": req.ID, "jsonrpc": "2.0", "result": result}
		if err := enc.Encode(resp); err != nil {
			return
		}
	}
}

func newFakeSpdkClient(c *C) (*spdkclient.Client, *fakeSpdk) {
	clientConn, serverConn := net.Pipe()
	fake := &fakeSpdk{}
	go fake.serve(serverConn)
	cli := spdkclient.NewClientWithConn(context.Background(), clientConn)
	return cli, fake
}

func newRebuildSrcTestReplica(c *C, transport NvmfTransportType, snapshotName string) *Replica {
	r := &Replica{
		Name:              "vol-a-r-0",
		IP:                "10.10.5.19",
		PortStart:         20000,
		PortEnd:           20005,
		ListenerTransport: transport,
		State:             types.InstanceStateRunning,
		SnapshotLvolMap:   map[string]*Lvol{},
		UpdateCh:          make(chan interface{}, 16),
		log:               log.NewSafeLogger(logrus.StandardLogger().WithField("test", true)),
	}
	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	r.SnapshotLvolMap[snapLvolName] = &Lvol{
		Name:  snapLvolName,
		UUID:  "11111111-2222-3333-4444-555555555555",
		Alias: "lvs/" + snapLvolName,
	}
	bitmap, err := commonbitmap.NewBitmap(r.rebuildPortAllocatorStart(), r.PortEnd)
	c.Assert(err, IsNil)
	r.portAllocator = bitmap
	return r
}

// On an RDMA node the rebuild src must expose the snapshot on its own
// transport at the allocated port AND add the TCP fallback listener at
// port+1 — matching the dial behavior of both old (unconditional +1
// fallback) and new (allowLegacyTCPFallback) peers.
func (s *TestSuite) TestRebuildingSrcStartExposesDualListenersOnRDMA(c *C) {
	fmt.Println("Testing RebuildingSrcStart dual-listener expose on RDMA")

	cli, fake := newFakeSpdkClient(c)
	defer func() {
		c.Assert(cli.Close(), IsNil)
	}()
	r := newRebuildSrcTestReplica(c, NvmfTransportRDMA, "snap1")

	addr, err := r.RebuildingSrcStart(cli, "vol-a-r-1", "10.10.3.19:20100", "snap1")
	c.Assert(err, IsNil)

	// RDMA nodes: local allocator starts at PortStart+2.
	expectedPort := r.PortStart + 2
	c.Check(addr, Equals, fmt.Sprintf("10.10.5.19:%d", expectedPort))

	snapNQN := helpertypes.GetNQN(GetReplicaSnapshotLvolName(r.Name, "snap1"))
	fake.Lock()
	defer fake.Unlock()
	c.Assert(fake.unknown, HasLen, 0, Commentf("unexpected RPCs: %v", fake.unknown))
	c.Assert(fake.subsystems, DeepEquals, []string{snapNQN})
	c.Assert(fake.listeners, DeepEquals, []fakeListener{
		{Nqn: snapNQN, Trtype: "rdma", Traddr: "10.10.5.19", Trsvcid: fmt.Sprintf("%d", expectedPort)},
		{Nqn: snapNQN, Trtype: "tcp", Traddr: "10.10.5.19", Trsvcid: fmt.Sprintf("%d", expectedPort+1)},
	})
}

// On a TCP-only node the fallback listener is a no-op: exactly one TCP
// listener at the allocated port.
func (s *TestSuite) TestRebuildingSrcStartExposesSingleListenerOnTCP(c *C) {
	fmt.Println("Testing RebuildingSrcStart single-listener expose on TCP")

	cli, fake := newFakeSpdkClient(c)
	defer func() {
		c.Assert(cli.Close(), IsNil)
	}()
	r := newRebuildSrcTestReplica(c, NvmfTransportTCP, "snap1")

	addr, err := r.RebuildingSrcStart(cli, "vol-a-r-1", "10.10.3.19:20100", "snap1")
	c.Assert(err, IsNil)

	// TCP nodes: local allocator starts at PortStart+1.
	expectedPort := r.PortStart + 1
	c.Check(addr, Equals, fmt.Sprintf("10.10.5.19:%d", expectedPort))

	snapNQN := helpertypes.GetNQN(GetReplicaSnapshotLvolName(r.Name, "snap1"))
	fake.Lock()
	defer fake.Unlock()
	c.Assert(fake.unknown, HasLen, 0, Commentf("unexpected RPCs: %v", fake.unknown))
	c.Assert(fake.listeners, DeepEquals, []fakeListener{
		{Nqn: snapNQN, Trtype: "tcp", Traddr: "10.10.5.19", Trsvcid: fmt.Sprintf("%d", expectedPort)},
	})
}
