package store

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

var (
	globalMutexDB sync.Mutex
	globalDB      *pebble.DB = nil
)

type MemoryStore struct {
	granularity  int64
	cardinality  int64
	currentTime  int64
	windows      map[string]*Window
	windowsIdxDB map[string]int
	idxWindowsDB map[int]string

	db *pebble.DB

	currentTimeKey []byte

	rwMutex sync.RWMutex
}

func NewMemoryStore(granularity int64, cardinality int64) *MemoryStore {
	if !validateConfigs(granularity, cardinality) {
		return nil
	}

	return &MemoryStore{
		granularity: granularity,
		cardinality: cardinality,
		db:          nil,
		windows:     make(map[string]*Window),
	}
}

func (store *MemoryStore) Tick() {
	store.rwMutex.RLock()

	currentTime := time.Now()
	//TODO mutex arround this update (is it even necessary? - unlikely to be concurrency and temporary incorrect value is good enough)
	for _, window := range store.windows {
		window.Tick(currentTime)
	}

	if store.db != nil {
		store.db.Set(store.currentTimeKey, store.safeMarshal(store.currentTime), pebble.NoSync)
	}

	store.rwMutex.RUnlock()
}

func (store *MemoryStore) Push(id string, t time.Time, metric any, lambda *gojq.Code) {
	store.rwMutex.RLock()

	window, ok := store.windows[id]
	if !ok {
		store.rwMutex.RUnlock()
		store.rwMutex.Lock()
		if _, ok := store.windows[id]; !ok {
			if store.db != nil {
				batch := store.db.NewBatch()
				batch.Set(store.lenWindowsKey(), store.safeMarshal(len(store.windows)+1), nil)
				batch.Set(store.windowIdxKey(len(store.windows)), store.safeMarshal(id), nil)

				if err := batch.Commit(pebble.NoSync); err != nil {
					logrus.Errorf("store Push commit failed: %+v", err)
				}

				store.windowsIdxDB[id] = len(store.windows)
				store.idxWindowsDB[len(store.windows)] = id
			}
			store.windows[id] = newWindow(store.cardinality, store.granularity, store.db)
		}
		store.rwMutex.Unlock()
		store.rwMutex.RLock()
		window = store.windows[id]
	}

	window.Push(t, metric)
	store.rwMutex.RUnlock()
}

func validateConfigs(granularity int64, cardinality int64) bool {
	return granularity > 0 && cardinality > 0
}

/*
 *	BD Keys
 */

func (store *MemoryStore) lenWindowsKey() []byte {
	return []byte(fmt.Sprintf("len_windows"))
}

func (store *MemoryStore) granularity_key() []byte {
	return []byte(fmt.Sprintf("granularity"))
}

func (store *MemoryStore) cardinality_key() []byte {
	return []byte(fmt.Sprintf("cardinality"))
}

func (store *MemoryStore) windowIdxKey(index int) []byte {
	return []byte(fmt.Sprintf("window/%d", index))
}

func (store *MemoryStore) generate_constants() {
	store.currentTimeKey = []byte(fmt.Sprintf("current_time"))
}

/*
 *	Loads/Persistence
 */

func (store *MemoryStore) activate_cached_persistence() {
	logrus.Infof("memmory try_load_from_db: Persiste namespace")
	batch := store.db.NewBatch()
	batch.Set(store.granularity_key(), store.safeMarshal(store.granularity), nil)
	batch.Set(store.cardinality_key(), store.safeMarshal(store.cardinality), nil)
	batch.Set(store.lenWindowsKey(), store.safeMarshal(0), nil)
	batch.Set(store.currentTimeKey, store.safeMarshal(0), nil)

	if err := batch.Commit(pebble.NoSync); err != nil {
		store.db = nil
		logrus.Errorf("memory activate_cached_persistence commit failed (using only memmory): %v", err)
	}
}

func (store *MemoryStore) try_load_from_db() (ok bool) {

	bd_granularity, closer_gran, err_gran := store.db.Get(store.granularity_key())
	bd_cardinality, closer_card, err_card := store.db.Get(store.cardinality_key())
	defer func() {
		if closer_gran != nil {
			if err := closer_gran.Close(); err != nil {
				logrus.Errorf("memory try_load_from_db close granularity: %+v", err)
			}
		}
		if closer_card != nil {
			if err := closer_card.Close(); err != nil {
				logrus.Errorf("memory try_load_from_db close cardinality: %+v", err)
			}
		}
	}()

	if err_gran == pebble.ErrNotFound || err_card == pebble.ErrNotFound {
		logrus.Debugf("memmory try_load_from_db: Namespace not persisted to pebble DB yet.")
		return false
	} else if err_gran != nil || err_card != nil {
		logrus.Errorf("memmory try_load_from_db: err_gran %+v err_card %+v", err_gran, err_card)
		return false
	}

	var my_gran, my_card int64
	store.safe_unmarshal(bd_granularity, &my_gran)
	store.safe_unmarshal(bd_cardinality, &my_card)

	if my_gran != store.granularity || my_card != store.cardinality {
		logrus.Errorf("memmory try_load_from_db: Config: {granularity:%d, cardinality:%d} BD: {granularity:%d, cardinality:%d}", store.granularity, store.cardinality, my_gran, my_card)
		return false
	}

	return store.try_load_windows_from_db()
}

func (store *MemoryStore) try_load_windows_from_db() (ok bool) {
	bd_current_time, closer, err := store.db.Get(store.currentTimeKey)
	if err != nil {
		logrus.Errorf("memory try_load_windows_from_db - Unable to get store current time: %+v", err)
		return false
	}
	var current_time int64
	store.safe_unmarshal(bd_current_time, &current_time)

	if err := closer.Close(); err != nil {
		logrus.Errorf("memory try_load_windows_from_db close current_time: %+v", err)
	}

	bd_number_windows, closer, err := store.db.Get(store.lenWindowsKey())
	if err != nil {
		logrus.Errorf("memory try_load_windows_from_db - Unable to get number of windows: %+v", err)
		return false
	}
	var number_windows int
	store.safe_unmarshal(bd_number_windows, &number_windows)

	if err := closer.Close(); err != nil {
		logrus.Errorf("memory try_load_windows_from_db close number_windows_key: %+v", err)
	}
	for idx := 0; idx < number_windows; idx++ {
		bd_id, closer, err := store.db.Get(store.windowIdxKey(idx))
		if err != nil {
			logrus.Errorf("memory try_load_windows_from_db - Unable to get windows idx %d: %+v", idx, err)
			for k := range store.windows {
				delete(store.windows, k)
			}
			for k := range store.windowsIdxDB {
				delete(store.windowsIdxDB, k)
			}
			for k := range store.idxWindowsDB {
				delete(store.idxWindowsDB, k)
			}
			return false
		}
		var id string
		store.safe_unmarshal(bd_id, &id)
		if err := closer.Close(); err != nil {
			logrus.Errorf("store try_load_windows_from_db close window id %d: %+v", idx, err)
		}
		store.windowsIdxDB[id] = idx
		store.idxWindowsDB[idx] = id

		store.windows[id] = newWindow(store.cardinality, store.granularity, store.db)
	}

	store.currentTime = current_time

	return true
}

/*
 *	Marshal
 */

func (store *MemoryStore) safeMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		logrus.Errorf("memory_store safe marshal: %+v", err)
		return []byte("")
	}
	return b
}

func (store *MemoryStore) safe_unmarshal(b []byte, v any) any {
	err := json.Unmarshal(b, v)
	if err != nil {
		logrus.Errorf("memory_store safe unmarshal: %+v", err)
		return nil
	}
	return v
}
