// Copyright 2022 The SODA Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	kahuv1beta1 "github.com/soda-cdm/kahu/apis/kahu/v1beta1"
	"github.com/soda-cdm/kahu/controllers"
	pkgbackup "github.com/soda-cdm/kahu/controllers"
	kahuclientset "github.com/soda-cdm/kahu/controllers/client/clientset/versioned"
	kahuv1client "github.com/soda-cdm/kahu/controllers/client/clientset/versioned/typed/kahu/v1beta1"
	kinf "github.com/soda-cdm/kahu/controllers/client/informers/externalversions/kahu/v1beta1"
	kahulister "github.com/soda-cdm/kahu/controllers/client/listers/kahu/v1beta1"
	collections "github.com/soda-cdm/kahu/utils"
	utils "github.com/soda-cdm/kahu/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	// kbclient "sigs.k8s.io/controller-runtime/pkg/client"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	metaservice "github.com/soda-cdm/kahu/providerframework/meta_service/lib/go"
	"google.golang.org/grpc"
)

var (
	address        = "10.96.167.143"
	port           = 8181
	grpcServer     *grpc.Server
	grpcConnection *grpc.ClientConn
	metaClient     metaservice.MetaServiceClient
)

type Controller struct {
	*controllers.BaseController
	client       kubernetes.Interface
	klient       kahuclientset.Interface
	backupSynced cache.InformerSynced
	kLister      kahulister.BackupLister
	clock        clock.Clock
	config       *restclient.Config
}

func NewController(klient kahuclientset.Interface, backupInformer kinf.BackupInformer, config *restclient.Config) *Controller {

	c := &Controller{
		BaseController: controllers.NewBaseController(controllers.Backup),
		klient:         klient,
		backupSynced:   backupInformer.Informer().HasSynced,
		kLister:        backupInformer.Lister(),
		clock:          &clock.RealClock{},
		config:         config,
	}

	backupInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.handleAdd,
			DeleteFunc: c.handleDel,
		},
	)

	return c
}

func (c *Controller) Run(ch chan struct{}) error {
	if ok := cache.WaitForCacheSync(ch, c.backupSynced); !ok {
		c.Logger.Infoln("cache was not sycned")
	}

	go wait.Until(c.worker, time.Second, ch)

	<-ch
	return nil
}

func (c *Controller) worker() {
	for c.processNextItem() {

	}
}

type restoreContext struct {
	namespaceClient corev1.NamespaceInterface
}

func (c *Controller) processNextItem() bool {

	item, shutDown := c.Wq.Get()
	if shutDown {
		return false
	}

	defer c.Wq.Forget(item)
	key, err := cache.MetaNamespaceKeyFunc(item)
	if err != nil {
		c.Logger.Errorf("error %s calling Namespace key func on cache for item", err.Error())
		return false
	}

	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		c.Logger.Errorf("splitting key into namespace and name, error %s\n", err.Error())
		return false
	}

	bkp, err := c.kLister.Backups(ns).Get(name)
	if err != nil {
		c.Logger.Errorf("error %s, Getting the backup resource from lister", err.Error())
		return false
	}

	if apierrors.IsNotFound(err) {
		c.Logger.Debugf("backup %s not found", name)
		return false
	}
	if err != nil {
		return false
	}

	c.Logger.Debug("Preparing backup request")
	request := c.prepareBackupRequest(bkp)

	if len(request.Status.ValidationErrors) > 0 {
		request.Status.Phase = kahuv1beta1.BackupPhaseFailedValidation
		c.Logger.Debugf("Validation errors: %s", request.Status.ValidationErrors)
		// return false
	} else {
		request.Status.Phase = kahuv1beta1.BackupPhaseInProgress
	}

	if err := c.runBackup(request); err != nil {
		c.Logger.WithError(err).Error("backup failed")
		request.Status.Phase = v1beta1.BackupPhaseFailed
	}

	c.Logger.Infof("Kahu backup spec: %+v\n", bkp.Spec)

	return true
}

func patchBackup(original, updated *v1beta1.Backup, client kahuv1client.BackupsGetter) (*v1beta1.Backup, error) {
	origBytes, err := json.Marshal(original)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling original backup")
	}

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling updated backup")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error creating json merge patch for backup")
	}

	res, err := client.Backups(original.Namespace).Patch(context.TODO(), original.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error patching backup")
	}

	return res, nil
}

func (c *Controller) runBackup(backup *pkgbackup.Request) error {
	c.Logger.WithField(controllers.Backup, utils.NamespaceAndName(backup)).Info("Setting up backup log")

	// var metaClient metaservice.MetaServiceClient

	grpcConnection, err := metaservice.NewLBDial(fmt.Sprintf("%s:%d", address, port),
		grpc.WithInsecure())
	if err != nil {
		c.Logger.Errorf("Error connecting meta service %s", err)
		return err
	}

	metaClient = metaservice.NewMetaServiceClient(grpcConnection)
	backupClient, err := metaClient.Backup(context.Background())
	if err != nil {
		c.Logger.Errorf("Error initiating meta service backup %s", err)
		return err
	}

	err = backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_Identifier{
			Identifier: &metaservice.BackupIdentifier{
				BackupHandle: backup.Name,
			},
		},
	})
	if err != nil {
		c.Logger.Errorf("Unable to connect metadata service %s", err)
	}

	k8sClinet, err := kubernetes.NewForConfig(c.config)
	if err != nil {
		c.Logger.Errorf("Error initiating kube client %s", err)
		return err
	}
	resource_data, err := k8sClinet.CoreV1().Pods("default").
		Get(context.TODO(), "pod1", metav1.GetOptions{})
	if err != nil {
		c.Logger.Errorf("Error getting kube client %s", err)
		return err
	}

	c.Logger.Infof("Sending data %s", resource_data)

	resourceData, err := json.Marshal(resource_data)
	err = backupClient.Send(&metaservice.BackupRequest{
		Backup: &metaservice.BackupRequest_BackupResource{
			BackupResource: &metaservice.BackResource{
				Resource: &metaservice.Resource{
					Name:    backup.Name,
					Group:   backup.GroupVersionKind().Group,
					Version: backup.APIVersion,
					Kind:    backup.Kind,
				},
				Data: resourceData,
			},
		},
	})

	_, err = backupClient.CloseAndRecv()
	return err
}

func (c *Controller) prepareBackupRequest(backup *kahuv1beta1.Backup) *pkgbackup.Request {
	request := &pkgbackup.Request{
		Backup: backup.DeepCopy(),
	}

	if backup.Spec.MetadataLocation == nil {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("MetadataLocation can not be empty"))
		return request
	}

	// Getting all information of cluster version - useful for future skip-level migration
	if request.Annotations == nil {
		request.Annotations = make(map[string]string)
	}

	// validate the included/excluded resources
	for _, err := range collections.ValidateIncludesExcludes(request.Spec.IncludedResources, request.Spec.ExcludedResources) {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("Invalid included/excluded resource lists: %v", err))
	}

	// validate the included/excluded namespaces
	for _, err := range collections.ValidateNamespaceIncludesExcludes(request.Spec.IncludedNamespaces, request.Spec.ExcludedNamespaces) {
		request.Status.ValidationErrors = append(request.Status.ValidationErrors, fmt.Sprintf("Invalid included/excluded namespace lists: %v", err))
	}

	return request
}

// NamespaceAndName returns a string in the format <namespace>/<name>
func NamespaceAndName(objMeta metav1.Object) string {
	if objMeta.GetNamespace() == "" {
		return objMeta.GetName()
	}
	return fmt.Sprintf("%s/%s", objMeta.GetNamespace(), objMeta.GetName())
}

func (c *Controller) handleAdd(obj interface{}) {
	c.Logger.Infoln("handleAdd was called")
	c.Wq.Add(obj)
}

func (c *Controller) handleDel(obj interface{}) {
	c.Logger.Infoln("handleDel was called")
	c.Wq.Add(obj)
}
