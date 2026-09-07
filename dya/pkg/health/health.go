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

package health

import (
	"sync"
	"time"
)

type Checker struct {
	mu         sync.RWMutex
	started    time.Time
	ready      bool
	components map[string]bool
}

func NewChecker() *Checker {
	return &Checker{
		started:    time.Now(),
		ready:      false,
		components: make(map[string]bool),
	}
}

func (c *Checker) SetReady(ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = ready
}

func (c *Checker) SetComponent(name string, healthy bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components[name] = healthy
}

func (c *Checker) IsHealthy() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, healthy := range c.components {
		if !healthy {
			return false
		}
	}
	return true
}

func (c *Checker) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

func (c *Checker) Uptime() time.Duration {
	return time.Since(c.started)
}
