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
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	cs "kubedb.dev/apimachinery/client/clientset/versioned"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog"
	"kmodules.xyz/client-go/tools/queue"
)

type Controller struct {
	kubeInformerFactory informers.SharedInformerFactory
	clientConfig        *rest.Config
	kubeClient          kubernetes.Interface
	dbClient            cs.Interface

	maxNumRequeues int
	numThreads     int
	watchNamespace string
	mysqlname      string

	// selector for event-handler of MySQL Pod
	selector labels.Selector
	// tweakListOptions for watcher
	tweakListOptions func(*metav1.ListOptions)

	// MySQL Pod
	podQueue           *queue.Worker
	podInformer        cache.SharedIndexInformer
	podNamespaceLister corelisters.PodNamespaceLister
}

func NewLabelController(
	kubeInformerFactory informers.SharedInformerFactory,
	clientConfig *rest.Config,
	kubeClient kubernetes.Interface,
	dbClient cs.Interface,
	maxNumRequeues int,
	numThreads int,
	watchNamespace string,
	dbName string,
) *Controller {
	return &Controller{
		kubeInformerFactory: kubeInformerFactory,
		clientConfig:        clientConfig,
		kubeClient:          kubeClient,
		dbClient:            dbClient,

		maxNumRequeues: maxNumRequeues,
		numThreads:     numThreads,
		selector: labels.SelectorFromSet(map[string]string{
			api.LabelDatabaseKind: api.ResourceKindMySQL,
			api.LabelDatabaseName: dbName,
		}),
		watchNamespace: watchNamespace,
		mysqlname:      dbName,
	}
}

func (c *Controller) initInformer() cache.SharedIndexInformer {
	return c.kubeInformerFactory.InformerFor(&corev1.Pod{}, func(client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
		return coreinformers.NewFilteredPodInformer(
			client,
			c.watchNamespace,
			resyncPeriod,
			cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
			c.tweakListOptions,
		)
	})
}

// Blocks caller. Intended to be called as a Go routine.
func (c *Controller) RunLabelController(stopCh <-chan struct{}) {
	go c.StartAndRunController(stopCh)

	<-stopCh
}

// StartAndRunControllers starts InformerFactory and runs queue.worker
func (c *Controller) StartAndRunController(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	c.kubeInformerFactory.Start(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	for t, v := range c.kubeInformerFactory.WaitForCacheSync(stopCh) {
		if !v {
			klog.Fatalf("%v timed out waiting for caches to sync", t)
			return
		}
	}

	c.Run(stopCh)

	<-stopCh
}

// Run runs queue.worker
func (c *Controller) Run(stopCh <-chan struct{}) {
	c.podQueue.Run(stopCh)

}
