package backup

import (
	"context"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"

	kahuapi "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
)

const (
	backupCacheNamespaceIndex             = "backup-cache-namespace-index"
	backupCachePVCIndex                   = "backup-cache-pvc-index"
	backupCachePVIndex                    = "backup-cache-pv-index"
	backupCacheObjectClusterResourceIndex = "backup-cache-cluster-resource-index"
)

type Context interface {
	Complete(backup *kahuapi.Backup) error
	IsComplete() bool
	GetNamespaces() []string
}

type backupContext struct {
	controller
	isBackupCacheComplete bool
	backupIndexCache      cache.Indexer
}

func newContext(backName string, ctrl *controller) Context {
	logger := ctrl.logger.WithField("backup", backName)
	return &backupContext{
		controller: controller{
			logger:               logger,
			genericController:    ctrl.genericController,
			kubeClient:           ctrl.kubeClient,
			dynamicClient:        ctrl.dynamicClient,
			backupClient:         ctrl.backupClient,
			backupLister:         ctrl.backupLister,
			backupLocationLister: ctrl.backupLocationLister,
			eventRecorder:        ctrl.eventRecorder,
		},
		backupIndexCache:      cache.NewIndexer(cache.MetaNamespaceKeyFunc, newBackupObjectIndexers(logger)),
		isBackupCacheComplete: false,
	}
}

func newBackupObjectIndexers(logger log.FieldLogger) cache.Indexers {
	return cache.Indexers{
		backupCacheNamespaceIndex: func(obj interface{}) ([]string, error) {
			switch obj.(type) {
			case *v1.Namespace, v1.Namespace:
				break
			default:
				return []string{}, nil
			}
			name, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Warningf("Unable to get Namespace key for indexing. %s", err)
				return []string{}, nil
			}

			return []string{name}, nil
		},
		backupCachePVCIndex: func(obj interface{}) ([]string, error) {
			switch obj.(type) {
			case *v1.PersistentVolumeClaim, v1.PersistentVolumeClaim:
				break
			default:
				return []string{}, nil
			}
			name, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Warningf("Unable to get PVC key for indexing. %s", err)
				return []string{}, nil
			}

			return []string{name}, nil
		},
		backupCachePVIndex: func(obj interface{}) ([]string, error) {
			switch obj.(type) {
			case *v1.PersistentVolume, v1.PersistentVolume:
				break
			default:
				return []string{}, nil
			}
			name, err := cache.MetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Warningf("Unable to get PV key for indexing. %s", err)
				return []string{}, nil
			}
			return []string{name}, nil
		},
		backupCacheObjectClusterResourceIndex: func(obj interface{}) ([]string, error) {
			keys := make([]string, 0)
			metadata, err := meta.Accessor(obj)
			if err != nil {
				logger.Warningf("Unable to get cluster scope key indexing. %s", err)
				return []string{}, nil
			}
			if len(metadata.GetNamespace()) == 0 {
				keys = append(keys, metadata.GetName())
			}
			return keys, nil
		},
	}
}

func (ctx *backupContext) Complete(backup *kahuapi.Backup) error {
	if ctx.isBackupCacheComplete {
		return nil
	}

	// if phase has not completed validation, ignore populating backup objects
	if backup.Status.Phase == kahuapi.BackupPhaseInit {
		return errors.New("backup has not finished validation")
	}

	// populate all backup resources in cache
	var err error
	if len(backup.Status.Resources) > 0 {
		err = ctx.populateCacheFromBackupStatus(backup)
	} else {
		err = ctx.populateCacheFromBackupSpec(backup)
	}

	if err == nil {
		ctx.isBackupCacheComplete = true
	}
	return err
}

func (ctx *backupContext) IsComplete() bool {
	return ctx.isBackupCacheComplete
}

func (ctx *backupContext) GetNamespaces() []string {
	return ctx.getCachedNamespaceNames()
}

func (ctx *backupContext) populateCacheFromBackupStatus(backup *kahuapi.Backup) error {

	return nil
}

func (ctx *backupContext) populateCacheFromBackupSpec(backup *kahuapi.Backup) error {
	// collect namespaces
	err := ctx.collectNamespacesFromSpec(backup)
	if err != nil {
		return err
	}

	// collect resources with namespaces
	err = ctx.collectResources(backup)
	if err != nil {
		return err
	}

	return nil
}

func (ctx *backupContext) collectNamespacesFromSpec(backup *kahuapi.Backup) error {
	// collect namespaces
	namespaces, err := ctx.kubeClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		ctx.logger.Errorf("Unable to list namespace. %s", err)
		return errors.Wrap(err, "unable to get namespaces")
	}

	namespaceList := make(map[string]v1.Namespace, 0)
	includeList := sets.NewString(backup.Spec.IncludeNamespaces...)
	excludeList := sets.NewString(backup.Spec.ExcludeNamespaces...)
	for _, namespace := range namespaces.Items {
		if excludeList.Has(namespace.Name) {
			continue
		}
		namespaceList[namespace.Name] = namespace
	}

	for _, name := range includeList.List() {
		_, ok := namespaceList[name]
		if !ok { // remove from list if not available in include list
			delete(namespaceList, name)
		}
	}

	for name, namespace := range namespaceList {
		err := ctx.backupIndexCache.Add(namespace.DeepCopy())
		if err != nil {
			ctx.logger.Warningf("Failed to add namespace(%s). %s", name, err)
		}
	}

	return nil
}

func (ctx *backupContext) getCachedNamespaceNames() []string {
	// collect namespace names
	return ctx.backupIndexCache.ListIndexFuncValues(backupCacheNamespaceIndex)
}

func (ctx *backupContext) collectResources(backup *kahuapi.Backup) error {
	// collect namespaces

	// ctx.

	return nil
}
