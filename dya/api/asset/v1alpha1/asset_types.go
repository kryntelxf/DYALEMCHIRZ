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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AssetSpec defines the desired state of Asset
type AssetSpec struct {
	// DisplayName is the human-readable name of the asset
	DisplayName string `json:"displayName,omitempty"`

	// AssetType is the type of asset (e.g., "Node", "Pod", "Service", "Database")
	AssetType string `json:"assetType"`

	// Parent is the reference to the parent asset
	Parent string `json:"parent,omitempty"`

	// Labels are key-value pairs for categorization
	Labels map[string]string `json:"labels,omitempty"`

	// Properties are additional properties of the asset
	Properties map[string]string `json:"properties,omitempty"`

	// Health represents the current health status
	Health HealthStatus `json:"health,omitempty"`
}

// HealthStatus represents the health of an asset
type HealthStatus struct {
	// Status is the overall status (Healthy, Degraded, Unhealthy, Unknown)
	Status string `json:"status,omitempty"`

	// LastUpdated is the timestamp of the last health update
	LastUpdated metav1.Time `json:"lastUpdated,omitempty"`

	// Message provides additional health information
	Message string `json:"message,omitempty"`
}

// AssetStatus defines the observed state of Asset
type AssetStatus struct {
	// ObservedGeneration is the generation observed by the controller
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// DependencyCount is the number of dependencies
	DependencyCount int `json:"dependencyCount,omitempty"`

	// LastSync is the timestamp of the last sync
	LastSync metav1.Time `json:"lastSync,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=assets,scope=Cluster
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.assetType`
// +kubebuilder:printcolumn:name="Health",type=string,JSONPath=`.spec.health.status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Asset is the Schema for the assets API
type Asset struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AssetSpec   `json:"spec,omitempty"`
	Status AssetStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AssetList contains a list of Asset
type AssetList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Asset `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Asset{}, &AssetList{})
}
