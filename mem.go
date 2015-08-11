// Package mem is an example REST backend storage that stores everything in memory.
package mem

import (
	"sort"
	"sync"
	"time"

	"github.com/rs/rest-layer/resource"
	"golang.org/x/net/context"
)

// MemoryHandler is an example handler storing data in memory
type MemoryHandler struct {
	sync.RWMutex
	// If latency is set, the handler will introduce an artificial latency on
	// all operations
	Latency time.Duration
	items   map[interface{}]resource.Item
	ids     []interface{}
}

// NewHandler creates an empty memory handler
func NewHandler() *MemoryHandler {
	return &MemoryHandler{
		items: map[interface{}]resource.Item{},
		ids:   []interface{}{},
	}
}

// NewSlowHandler creates an empty memory handler with specified latency
func NewSlowHandler(latency time.Duration) *MemoryHandler {
	return &MemoryHandler{
		Latency: latency,
		items:   map[interface{}]resource.Item{},
		ids:     []interface{}{},
	}
}

// Insert inserts new items in memory
func (m *MemoryHandler) Insert(ctx context.Context, items []*resource.Item) (err error) {
	m.Lock()
	defer m.Unlock()
	err = handleWithLatency(m.Latency, ctx, func() error {
		for _, item := range items {
			if _, found := m.items[item.ID]; found {
				return resource.ErrConflict
			}
		}
		for _, item := range items {
			// Store ids in ordered slice for sorting
			m.ids = append(m.ids, item.ID)
			m.items[item.ID] = *item
		}
		return nil
	})
	return err
}

// Update replace an item by a new one in memory
func (m *MemoryHandler) Update(ctx context.Context, item *resource.Item, original *resource.Item) (err error) {
	m.Lock()
	defer m.Unlock()
	err = handleWithLatency(m.Latency, ctx, func() error {
		o, found := m.items[original.ID]
		if !found {
			return resource.ErrNotFound
		}
		if original.ETag != o.ETag {
			return resource.ErrConflict
		}
		m.items[item.ID] = *item
		return nil
	})
	return err
}

// Delete deletes an item from memory
func (m *MemoryHandler) Delete(ctx context.Context, item *resource.Item) (err error) {
	m.Lock()
	defer m.Unlock()
	err = handleWithLatency(m.Latency, ctx, func() error {
		o, found := m.items[item.ID]
		if !found {
			return resource.ErrNotFound
		}
		if item.ETag != o.ETag {
			return resource.ErrConflict
		}
		m.delete(item.ID)
		return nil
	})
	return err
}

// Clear clears all items from the memory store matching the lookup
func (m *MemoryHandler) Clear(ctx context.Context, lookup *resource.Lookup) (total int, err error) {
	m.Lock()
	defer m.Unlock()
	err = handleWithLatency(m.Latency, ctx, func() error {
		for _, id := range m.ids {
			item := m.items[id]
			if !lookup.Filter().Match(item.Payload) {
				continue
			}
			m.delete(item.ID)
			total++
		}
		return nil
	})
	return total, err
}

// delete removes an item by this id with no look
func (m *MemoryHandler) delete(id interface{}) {
	delete(m.items, id)
	// Remove id from id list
	for i, _id := range m.ids {
		if _id == id {
			if i >= len(m.ids)-1 {
				m.ids = m.ids[:i]
			} else {
				m.ids = append(m.ids[:i], m.ids[i+1:]...)
			}
		}
	}
}

// Find items from memory matching the provided lookup
func (m *MemoryHandler) Find(ctx context.Context, lookup *resource.Lookup, page, perPage int) (list *resource.ItemList, err error) {
	m.RLock()
	defer m.RUnlock()
	err = handleWithLatency(m.Latency, ctx, func() error {
		items := []*resource.Item{}
		// Apply filter
		for _, id := range m.ids {
			item := m.items[id]
			if !lookup.Filter().Match(item.Payload) {
				continue
			}
			items = append(items, &item)
		}
		// Apply sort
		if len(lookup.Sort()) > 0 {
			s := sortableItems{lookup.Sort(), items}
			sort.Sort(s)
		}
		// Apply pagination
		total := len(items)
		start := (page - 1) * perPage
		end := total
		if perPage > 0 {
			end = start + perPage
			if start > total-1 {
				start = 0
				end = 0
			} else if end > total-1 {
				end = total
			}
		}
		list = &resource.ItemList{total, page, items[start:end]}
		return nil
	})
	return list, err
}
