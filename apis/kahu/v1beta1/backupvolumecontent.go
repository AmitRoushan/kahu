/*
Copyright 2022 The SODA Authors.

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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupVolumeContentSpec defines the desired state of BackupVolumeContent
type BackupVolumeContentSpec struct {
	// BackupName is backup CR name specified during backup
	// +required
	BackupName string `json:"backupName"`

	// Volume represents kubernetes volume to be backed up
	// +required
	Volume v1.PersistentVolume `json:"volume"`
}

// +kubebuilder:validation:Enum=New;InProgress;Completed;Failed;Deleting

type BackupVolumeContentPhase string

const (
	BackupVolumeContentPhaseInit       BackupVolumeContentPhase = "New"
	BackupVolumeContentPhaseInProgress BackupVolumeContentPhase = "InProgress"
	BackupVolumeContentPhaseCompleted  BackupVolumeContentPhase = "Completed"
	BackupVolumeContentPhaseFailed     BackupVolumeContentPhase = "Failed"
	BackupVolumeContentPhaseDeleting   BackupVolumeContentPhase = "Deleting"
)

// BackupVolumeContentStatus defines the observed state of BackupVolumeContent
type BackupVolumeContentStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// +optional
	// +kubebuilder:default=New
	Phase BackupVolumeContentPhase `json:"phase,omitempty"`

	// +optional
	// +nullable
	StartTimestamp *metav1.Time `json:"startTimestamp,omitempty"`

	// +optional
	// +nullable
	CompletionTimestamp *metav1.Time `json:"completionTimestamp,omitempty"`

	// +optional
	FailureReason string `json:"failureReason,omitempty"`

	// +optional
	VolumeProvider string `json:"volumeProvider,omitempty"`

	// +optional
	VolumeType string `json:"volumeType,omitempty"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:skipVerbs=update,patch
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// BackupVolumeContent is the Schema for the BackupVolumeContents API
type BackupVolumeContent struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec BackupVolumeContentSpec `json:"spec,omitempty"`
	// +optional
	Status BackupVolumeContentStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// BackupVolumeContentList contains a list of BackupVolumeContent
type BackupVolumeContentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupVolumeContent `json:"items"`
}
