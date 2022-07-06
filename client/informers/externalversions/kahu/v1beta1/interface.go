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

// Code generated by informer-gen. DO NOT EDIT.

package v1beta1

import (
	internalinterfaces "github.com/soda-cdm/kahu/client/informers/externalversions/internalinterfaces"
)

// Interface provides access to all the informers in this group version.
type Interface interface {
	// Backups returns a BackupInformer.
	Backups() BackupInformer
	// BackupLocations returns a BackupLocationInformer.
	BackupLocations() BackupLocationInformer
	// BackupVolumeContents returns a BackupVolumeContentInformer.
	BackupVolumeContents() BackupVolumeContentInformer
	// Providers returns a ProviderInformer.
	Providers() ProviderInformer
	// Restores returns a RestoreInformer.
	Restores() RestoreInformer
}

type version struct {
	factory          internalinterfaces.SharedInformerFactory
	namespace        string
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// New returns a new Interface.
func New(f internalinterfaces.SharedInformerFactory, namespace string, tweakListOptions internalinterfaces.TweakListOptionsFunc) Interface {
	return &version{factory: f, namespace: namespace, tweakListOptions: tweakListOptions}
}

// Backups returns a BackupInformer.
func (v *version) Backups() BackupInformer {
	return &backupInformer{factory: v.factory, tweakListOptions: v.tweakListOptions}
}

// BackupLocations returns a BackupLocationInformer.
func (v *version) BackupLocations() BackupLocationInformer {
	return &backupLocationInformer{factory: v.factory, tweakListOptions: v.tweakListOptions}
}

// BackupVolumeContents returns a BackupVolumeContentInformer.
func (v *version) BackupVolumeContents() BackupVolumeContentInformer {
	return &backupVolumeContentInformer{factory: v.factory, tweakListOptions: v.tweakListOptions}
}

// Providers returns a ProviderInformer.
func (v *version) Providers() ProviderInformer {
	return &providerInformer{factory: v.factory, tweakListOptions: v.tweakListOptions}
}

// Restores returns a RestoreInformer.
func (v *version) Restores() RestoreInformer {
	return &restoreInformer{factory: v.factory, tweakListOptions: v.tweakListOptions}
}
