// Package dsstate implements the IPFS Cluster state interface using
// an underlying go-datastore.
package dsstate

import (
	"context"
	"errors"
	"io"

	"github.com/ipfs/ipfs-cluster/api"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	logging "github.com/ipfs/go-log"

	codec "github.com/ugorji/go/codec"
)

var logger = logging.Logger("dsstate")

// State implements the IPFS Cluster state interface.
type State struct {
	ds          ds.Datastore
	codecHandle codec.Handle
	namespace   ds.Key
	version     int
}

// DefaultHandler returns the codec handler of choice (Msgpack).
func DefaultHandle() codec.Handle {
	h := &codec.MsgpackHandle{}
	return h
}

// New returns a new state using the given datastore.
// TODO: Consider supporting GC datastores.
func New(dstore ds.Datastore, version int, namespace string, handle codec.Handle) (*State, error) {
	if handle == nil {
		handle = DefaultHandle()
	}

	st := &State{
		ds:          dstore,
		codecHandle: handle,
		namespace:   ds.NewKey(namespace),
	}

	curVersion := st.GetVersion()
	if curVersion < 0 {
		return nil, errors.New("error reading state version")
	}

	// initialize
	if curVersion == 0 {
		err := st.SetVersion(version)
		if err != nil {
			return nil, err
		}
	}
	return st, nil
}

// Add
func (st *State) Add(c api.Pin) error {
	ps, err := st.serializePin(&c)
	if err != nil {
		return err
	}
	return st.ds.Put(st.key(c.Cid), ps)
}

// Rm
func (st *State) Rm(c cid.Cid) error {
	err := st.ds.Delete(st.key(c))
	if err == ds.ErrNotFound {
		return nil
	}
	return err
}

func (st *State) Get(c cid.Cid) (api.Pin, bool) {
	v, err := st.ds.Get(st.key(c))
	if err != nil {
		return api.PinCid(c), false
	}
	p, err := st.deserializePin(c, v)
	if err != nil {
		return api.PinCid(c), false
	}
	return *p, true
}

func (st *State) Has(c cid.Cid) bool {
	ok, err := st.ds.Has(st.key(c))
	if err != nil {
		logger.Error(err)
	}
	return ok && err == nil
}

func (st *State) List() []api.Pin {
	q := query.Query{
		Prefix: st.namespace.String(),
	}

	results, err := st.ds.Query(q)
	if err != nil {
		return []api.Pin{}
	}
	defer results.Close()

	var pins []api.Pin
	versionKey := st.versionKey()

	for r := range results.Next() {
		if r.Error != nil {
			logger.Errorf("error in query result: %s", r.Error)
			return pins
		}
		k := ds.NewKey(r.Key)
		if k.Equal(versionKey) {
			continue
		}

		ci, err := st.unkey(k)
		if err != nil {
			logger.Error("key: ", k, "error: ", err)
			logger.Error(string(r.Value))
			continue
		}

		p, err := st.deserializePin(ci, r.Value)
		if err != nil {
			logger.Errorf("error deserializing pin (%s): %s", r.Key, err)
			continue
		}

		pins = append(pins, *p)
	}
	return pins
}

// Migrate is a no-op for now.
func (st *State) Migrate(r io.Reader) error {
	return nil
}

func (st *State) versionKey() ds.Key {
	return st.namespace.Child(ds.NewKey("/version"))
}

func (st *State) GetVersion() int {
	v, err := st.ds.Get(st.versionKey())
	if err != nil {
		if err == ds.ErrNotFound {
			return 0 // fine
		}
		logger.Error("error getting version: ", err)
		return -1
	}
	if len(v) != 1 {
		logger.Error("bad version length")
		return -1
	}
	return int(v[0])
}

func (st *State) SetVersion(v int) error {
	err := st.ds.Put(st.versionKey(), []byte{byte(v)})
	if err != nil {
		logger.Error("error storing version:", v)
		return err
	}
	st.version = v
	return nil
}

type serialEntry struct {
	Key   string `codec:"k"`
	Value []byte `codec:"v"`
}

// Marshal dumps the state to a writer.
func (st *State) Marshal(ctx context.Context, w io.Writer) error {
	q := query.Query{
		Prefix: st.namespace.String(),
	}

	results, err := st.ds.Query(q)
	if err != nil {
		return err
	}
	defer results.Close()

	enc := codec.NewEncoder(w, st.codecHandle)

	for r := range results.Next() {
		if r.Error != nil {
			logger.Errorf("error in query result: %s", r.Error)
			return r.Error
		}

		k := ds.NewKey(r.Key)
		// reduce snapshot size by not storing the prefix
		err := enc.Encode(serialEntry{
			Key:   k.BaseNamespace(),
			Value: r.Value,
		})
		if err != nil {
			logger.Error(err)
			return err
		}
	}
	return nil
}

// Unmarshal from the given reader.
// As of now, it does not drop the previous state, only adds/overwrites it.
func (st *State) Unmarshal(ctx context.Context, r io.Reader) error {
	dec := codec.NewDecoder(r, st.codecHandle)
	for {
		var entry serialEntry
		if err := dec.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		k := st.namespace.Child(ds.NewKey(entry.Key))
		err := st.ds.Put(k, entry.Value)
		if err != nil {
			logger.Error("error adding unmarshaled key to datastore:", err)
			return err
		}
	}

	return nil
}

// /namespace/cidKey
func (st *State) key(c cid.Cid) ds.Key {
	k := dshelp.CidToDsKey(c)
	return st.namespace.Child(k)
}

func (st *State) unkey(k ds.Key) (cid.Cid, error) {
	return dshelp.DsKeyToCid(ds.NewKey(k.BaseNamespace()))
}

func (st *State) serializePin(c *api.Pin) ([]byte, error) {
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, st.codecHandle)
	//defer enc.Release() // should happen automatically
	ps := c.ToSerial()
	ps.Cid = "" // save space, we never store the CID.
	err := enc.Encode(ps)
	if err != nil {
		logger.Error(err)
	}
	return buf, err
}

func (st *State) deserializePin(c cid.Cid, buf []byte) (*api.Pin, error) {
	dec := codec.NewDecoderBytes(buf, st.codecHandle)
	var ps api.PinSerial
	err := dec.Decode(&ps)
	if err != nil {
		logger.Error(err)
		return nil, err
	}
	p := ps.ToPin()
	p.Cid = c
	//defer dec.Release() // should happen automatically
	return &p, err
}
