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

package buffers

import (
	"sync"

	"k8s.io/klog/v2"
)

type BasicBuffer struct {
	mu   sync.RWMutex
	data []interface{}
}

func NewBasicBuffer() *BasicBuffer {
	return &BasicBuffer{
		data: make([]interface{}, 0),
	}
}

func (b *BasicBuffer) Name() string {
	return "basic-buffer"
}

func (b *BasicBuffer) Store(data interface{}) error {
	if data == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, data)
	klog.V(4).Infof("Buffer stored %d items", len(b.data))
	return nil
}

func (b *BasicBuffer) Flush() ([]interface{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) == 0 {
		return nil, nil
	}
	result := b.data
	b.data = make([]interface{}, 0)
	klog.V(4).Infof("Buffer flushed %d items", len(result))
	return result, nil
}
