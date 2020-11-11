/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	core_util "kmodules.xyz/client-go/core/v1"
	"kmodules.xyz/client-go/tools/queue"
)

func (c *Controller) initWatcher() {
	c.podInformer = c.initInformer()
	c.podQueue = queue.New("Pod", c.maxNumRequeues, c.numThreads, c.podLabeler)
	c.podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if pod, ok := obj.(*core.Pod); ok {
				queue.Enqueue(c.podQueue.GetQueue(), pod)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			if pod, ok := newObj.(*core.Pod); ok {
				queue.Enqueue(c.podQueue.GetQueue(), pod)
			}

		},
		DeleteFunc: func(obj interface{}) {
			if pod, ok := obj.(*core.Pod); ok {
				queue.Enqueue(c.podQueue.GetQueue(), pod)
			}
		},
	})
	c.podNamespaceLister = c.kubeInformerFactory.Core().V1().Pods().Lister().Pods(c.watchNamespace)
}

func (c *Controller) podLabeler(key string) error {
	selfKey := fmt.Sprintf("%s/%s", c.namespace, c.podName)
	if key != selfKey {
		key = selfKey
	}
	klog.Infoln("Started processing, key:", key)
	obj, exists, err := c.podInformer.GetIndexer().GetByKey(key)
	if err != nil {
		klog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return err
	}

	if !exists {
		klog.Infof("Pod %s does not exist anymore", key)
	} else {
		pod := obj.(*core.Pod).DeepCopy()

		isPrimary, err := c.checkPrimary(pod.ObjectMeta)
		if err != nil {
			return err
		}
		if isPrimary {
			if err := c.removeInvalidPrimaryLabel(pod); err != nil {
				return err
			}
			if err := c.ensurePrimaryRoleLabel(pod); err != nil {
				return err
			}
			klog.Infof("Adding label: role=primary to Pod %s/%s has succeeded!!", pod.Namespace, pod.Name)
		} else {
			if err := c.ensureStandbyRoleLabel(pod); err != nil {
				return err
			}
			klog.Infof("Adding label: role=secondary to Pod %s/%s has succeeded!!", pod.Namespace, pod.Name)
		}
	}
	return nil
}

func (c *Controller) checkPrimary(podMeta metav1.ObjectMeta) (bool, error) {
	if c.dbType == api.ResourceKindMySQL {
		result, err := c.queryInMySQLDatabase(podMeta)
		if err != nil {
			return false, err
		}

		host := string(result[0]["MEMBER_HOST"])
		hostName := strings.Split(host, ".")[0]

		if hostName == podMeta.Name {
			return true, nil
		}
	} else if c.dbType == api.ResourceKindMongoDB {
		return c.IsMaster(podMeta)
	}

	return false, nil
}

func (c *Controller) ensurePrimaryRoleLabel(pod *core.Pod) error {
	_, _, err := core_util.PatchPod(context.TODO(), c.kubeClient, pod, func(in *core.Pod) *core.Pod {
		in.Labels = core_util.UpsertMap(in.Labels, map[string]string{
			api.LabelRole: api.DatabasePodPrimary,
		})
		return in
	}, metav1.PatchOptions{})

	return err
}

func (c *Controller) ensureStandbyRoleLabel(pod *core.Pod) error {
	_, _, err := core_util.PatchPod(context.TODO(), c.kubeClient, pod, func(in *core.Pod) *core.Pod {
		in.Labels = core_util.UpsertMap(in.Labels, map[string]string{
			api.LabelRole: api.DatabasePodStandby,
		})
		return in
	}, metav1.PatchOptions{})
	return err
}

func (c *Controller) removeInvalidPrimaryLabel(primaryPod *core.Pod) error {
	podList, err := c.podNamespaceLister.List(labels.SelectorFromSet(getPrimaryLabels(c.dbName, c.dbType)))
	if err != nil {
		return err
	}
	for _, pod := range podList {
		if primaryPod.Name != pod.Name {
			_, _, err := core_util.PatchPod(context.TODO(), c.kubeClient, pod, func(in *core.Pod) *core.Pod {
				delete(in.Labels, api.LabelRole)
				return in
			}, metav1.PatchOptions{})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getPrimaryLabels(name, typ string) map[string]string {
	return map[string]string{
		api.LabelDatabaseName: name,
		api.LabelDatabaseKind: typ,
		api.LabelRole:         api.DatabasePodPrimary,
	}
}
