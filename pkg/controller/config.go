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

	cs "kubedb.dev/apimachinery/client/clientset/versioned"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Config struct {
	KubeInformerFactory informers.SharedInformerFactory
	ClientConfig        *rest.Config
	KubeClient          kubernetes.Interface
	DBClient            cs.Interface

	OperatorNamespace string
	ResyncPeriod      time.Duration
	MaxNumRequeues    int
	NumThreads        int
	WatchNamespace    string
	MySQLName         string
}

func (c *Config) New() (*Controller, error) {
	ctrl := NewLabelController(
		c.KubeInformerFactory,
		c.ClientConfig,
		c.KubeClient,
		c.DBClient,
		c.MaxNumRequeues,
		c.NumThreads,
		c.WatchNamespace,
		c.MySQLName,
	)

	ctrl.tweakListOptions = func(options *metav1.ListOptions) {
		options.LabelSelector = ctrl.selector.String()
	}

	ctrl.initWatcher()

	return ctrl, nil
}
