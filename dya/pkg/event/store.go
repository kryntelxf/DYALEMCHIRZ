/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package event

import (
	"sync"
	"time"
)

// Store stores events
type Store struct {
	mu     sync.RWMutex
	events []*Event
	limit  int
}

// NewStore creates a new event store
func NewStore(limit int) *Store {
	if limit <= 0 {
		limit = 1000
	}
	return &Store{
		events: make([]*Event, 0),
		limit:  limit,
	}
}

// Add adds an event to the store
func (s *Store) Add(event *Event) {
	if event == nil || s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, event)

	if len(s.events) > s.limit {
		s.events = s.events[len(s.events)-s.limit:]
	}
}

// GetRecent returns recent events
func (s *Store) GetRecent(limit int) []*Event {
	if s == nil {
		return []*Event{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 || limit > len(s.events) {
		limit = len(s.events)
	}
	if limit == 0 {
		return []*Event{}
	}
	return s.events[len(s.events)-limit:]
}

// GetByAsset returns events for a specific asset
func (s *Store) GetByAsset(assetID string) []*Event {
	if s == nil || assetID == "" {
		return []*Event{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Event, 0)
	for _, e := range s.events {
		if e != nil && e.AssetID == assetID {
			result = append(result, e)
		}
	}
	return result
}

// GetByTime returns events within a time range
func (s *Store) GetByTime(start, end time.Time) []*Event {
	if s == nil {
		return []*Event{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*Event, 0)
	for _, e := range s.events {
		if e != nil && e.Timestamp.After(start) && e.Timestamp.Before(end) {
			result = append(result, e)
		}
	}
	return result
}

// Count returns the number of events
func (s *Store) Count() int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}

// Clear removes all events
func (s *Store) Clear() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = make([]*Event, 0)
}
