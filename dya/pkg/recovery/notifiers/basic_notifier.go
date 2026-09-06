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

package notifiers

import (
	"k8s.io/klog/v2"
)

type BasicNotifier struct{}

func (n *BasicNotifier) Name() string {
	return "basic-notifier"
}

func (n *BasicNotifier) Notify(message string, level string) error {
	klog.V(4).Infof("[%s] Notification: %s", level, message)
	return nil
}
