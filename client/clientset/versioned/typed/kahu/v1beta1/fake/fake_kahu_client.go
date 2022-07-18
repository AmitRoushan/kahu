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

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	v1beta1 "github.com/soda-cdm/kahu/client/clientset/versioned/typed/kahu/v1beta1"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeKahuV1beta1 struct {
	*testing.Fake
}

func (c *FakeKahuV1beta1) Backups() v1beta1.BackupInterface {
	return &FakeBackups{c}
}

func (c *FakeKahuV1beta1) BackupLocations() v1beta1.BackupLocationInterface {
	return &FakeBackupLocations{c}
}

func (c *FakeKahuV1beta1) Providers() v1beta1.ProviderInterface {
	return &FakeProviders{c}
}

func (c *FakeKahuV1beta1) Restores() v1beta1.RestoreInterface {
	return &FakeRestores{c}
}

func (c *FakeKahuV1beta1) VolumeBackupContents() v1beta1.VolumeBackupContentInterface {
	return &FakeVolumeBackupContents{c}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeKahuV1beta1) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}
