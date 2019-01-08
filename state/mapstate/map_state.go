// Package mapstate implements the State interface for IPFS Cluster by using
// a map to keep track of the consensus-shared state.
package mapstate

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/state"
	"github.com/ipfs/ipfs-cluster/state/dsstate"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	sync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log"
)

// Version is the map state Version. States with old versions should
// perform an upgrade before.
const Version = 6

var logger = logging.Logger("mapstate")

// MapState is mostly a MapDatastore to store the pinset.
type MapState struct {
	dst *dsstate.State
}

// NewMapState initializes the internal map and returns a new MapState object.
func NewMapState() state.State {
	// We need to keep a custom Migrate until everyone is
	// on version 6. Then we could fully remove all the
	// wrapping struct and just return the dsstate directly.

	mapDs := ds.NewMapDatastore()
	mutexDs := sync.MutexWrap(mapDs)

	dsSt, err := dsstate.New(mutexDs, Version, "", dsstate.DefaultHandle())
	if err != nil {
		panic(err)
	}
	return &MapState{dsSt}
}

// Add adds a Pin to the internal map.
func (st *MapState) Add(c api.Pin) error {
	return st.dst.Add(c)
}

// Rm removes a Cid from the internal map.
func (st *MapState) Rm(c cid.Cid) error {
	return st.dst.Rm(c)
}

// Get returns Pin information for a CID.
// The returned object has its Cid and Allocations
// fields initialized, regardless of the
// presence of the provided Cid in the state.
// To check the presence, use MapState.Has(cid.Cid).
func (st *MapState) Get(c cid.Cid) (api.Pin, bool) {
	return st.dst.Get(c)
}

// Has returns true if the Cid belongs to the State.
func (st *MapState) Has(c cid.Cid) bool {
	return st.dst.Has(c)
}

// List provides the list of tracked Pins.
func (st *MapState) List() []api.Pin {
	return st.dst.List()
}

// Migrate restores a snapshot from the state's internal bytes and if
// necessary migrates the format to the current version.
func (st *MapState) Migrate(r io.Reader) error {
	// TODO: Remove after migration to v6!
	// Read the full state - Unfortunately there is no way to
	// migrate v5 to v6 without doing this.
	full, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Try to do our special unmarshal
	buf := bytes.NewBuffer(full)
	err = st.Unmarshal(context.TODO(), buf)
	if err != nil {
		return err
	}

	v := st.GetVersion()

	if v == Version { // Unmarshal worked
		return nil
	}

	// we need to migrate. Discard first byte.
	buf2 := bytes.NewBuffer(full[1:])
	err = st.migrateFrom(v, buf2)
	if err != nil {
		return err
	}
	st.dst.SetVersion(Version)
	return nil
}

// GetVersion returns the current version of this state object.
// It is not necessarily up to date
func (st *MapState) GetVersion() int {
	return st.dst.GetVersion()
}

// Marshal encodes the state to the given writer. This implements
// go-libp2p-raft.Marshable.
func (st *MapState) Marshal(ctx context.Context, w io.Writer) error {
	return st.dst.Marshal(ctx, w)
}

// Unmarshal decodes the state from the given reader. This implements
// go-libp2p-raft.Marshable.
func (st *MapState) Unmarshal(ctx context.Context, r io.Reader) error {
	// TODO: Re-do when on version 6.
	// This is only to enable migration.

	// Extract the first byte in case this is an old
	// state.
	iobuf := bufio.NewReader(r)
	vByte, err := iobuf.ReadByte()
	if err != nil {
		return err
	}
	err = iobuf.UnreadByte()
	if err != nil {
		return err
	}

	// Try to unmarshal normally
	err = st.dst.Unmarshal(ctx, iobuf)
	if err == nil {
		return nil
	}

	// Error. Assume it's an old state
	// and try to migrate it.
	err = st.dst.SetVersion(int(vByte))
	if err != nil {
		return err
	}
	// We set the version but did not unmarshal.
	// This signals that a migration is needed.
	return nil
}
