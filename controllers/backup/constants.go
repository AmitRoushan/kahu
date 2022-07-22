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

package backup

import (
	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"time"
)

const (
	controllerName           = "backup-controller"
	backupFinalizer          = "kahu.io/backup-protection"
	defaultReconcileTimeLoop = 1 * time.Second

	backupCacheNamespaceIndex = "backup-cache-namespace-index"
	backupCacheResourceIndex  = "backup-cache-resource-index"
	//backupCachePVCIndex                   = "backup-cache-pvc-index"
	//backupCachePVIndex                    = "backup-cache-pv-index"
	backupCacheObjectClusterResourceIndex = "backup-cache-cluster-resource-index"

	volumeContentBackupLabel       = "kahu.io/backup-name"
	volumeContentVolumeProvider    = "kahu.io/backup-provider"
	annVolumeBackupDeleteCompleted = "kahu.io/volume-backup-delete-completed"
	annVolumeBackupCompleted       = "kahu.io/volume-backup-completed"
)

type Phase int

func toOrdinal(p kahuapi.BackupStage) Phase {
	switch p {
	case kahuapi.BackupStageInitial:
		return 0
	case kahuapi.BackupStagePreHook:
		return 1
	case kahuapi.BackupStageVolumes:
		return 2
	case kahuapi.BackupStagePostHook:
		return 3
	case kahuapi.BackupStageResources:
		return 4
	case kahuapi.BackupStageFinished:
		return 5
	}
	return -1
}

const (
	PVCKind   = "PersistentVolumeClaim"
	PVKind    = "PersistentVolume"
	NodeKind  = "Node"
	EventKind = "Event"
)
