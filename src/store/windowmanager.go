package store

import (
	"strings"
	"sync"
	"time"
)

type WindowManager struct {
	metricType  string
	cardinality int64
	granularity int64
	windows     map[string]*Window
	mutex       sync.Mutex
}

func NewWindowManager(metricType string, cardinality int64, granularity int64) *WindowManager {
	return &WindowManager{
		metricType:  metricType,
		cardinality: cardinality,
		granularity: granularity,
		windows:     make(map[string]*Window),
	}
}

func (wm *WindowManager) Tick() {
	wm.mutex.Lock()
	defer wm.mutex.Unlock()

	wm.tick()
}

func (wm *WindowManager) tick() {
	for _, window := range wm.windows {
		window.Tick()
	}
}

func (wm *WindowManager) UpdateWithLabels(t time.Time, metric any, labels map[string]string) {
	key := generateKey(labels)

	existingWindow, exists := wm.windows[key]
	if !exists {
		wm.mutex.Lock()
		wm.windows[key] = newWindow(wm.metricType, wm.cardinality, wm.granularity)
		existingWindow = wm.windows[key]
		defer wm.mutex.Unlock()
	}

	existingWindow.Update(t, metric)
}

func generateKey(labels map[string]string) string {
	var keys []string
	for k, v := range labels {
		keys = append(keys, k+"="+v)
	}
	return strings.Join(keys, ",")
}
