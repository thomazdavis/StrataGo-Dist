package stratago

import "github.com/thomazdavis/stratago/wal"

func (db *StrataGo) GetWAL() *wal.WAL {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.wal
}

func (db *StrataGo) GetActiveContents() map[string][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	res := make(map[string][]byte)
	iter := db.activeMemtable.NewIterator()
	for iter.Next() {
		res[string(iter.Key())] = iter.Value()
	}
	return res
}

func (db *StrataGo) GetImmutableContents() map[string][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.immutableMemtable == nil {
		return nil
	}
	res := make(map[string][]byte)
	iter := db.immutableMemtable.NewIterator()
	for iter.Next() {
		res[string(iter.Key())] = iter.Value()
	}
	return res
}

func (db *StrataGo) GetSSTableContents() map[string]map[string][]byte {
	db.mu.RLock()
	defer db.mu.RUnlock()
	res := make(map[string]map[string][]byte)
	for _, r := range db.sstReaders {
		res[r.Path()], _ = r.ReadAll()
	}
	return res
}
