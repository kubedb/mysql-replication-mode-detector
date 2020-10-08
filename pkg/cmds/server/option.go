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

package server

import (
	"flag"
	"io"
	"time"

	cs "kubedb.dev/apimachinery/client/clientset/versioned"
	"kubedb.dev/mysql-replication-mode-detector/pkg/controller"

	"github.com/spf13/pflag"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"kmodules.xyz/client-go/meta"
)

type Options struct {
	LabelerNamespace            string
	RestrictToOperatorNamespace bool
	QPS                         float64
	Burst                       int
	ResyncPeriod                time.Duration
	MaxNumRequeues              int
	NumThreads                  int

	// MySQL CR name
	DBName string

	StdOut io.Writer
	StdErr io.Writer
}

func NewOptions(out, errOut io.Writer) *Options {
	return &Options{
		LabelerNamespace: meta.Namespace(),
		// ref: https://github.com/kubernetes/ingress-nginx/blob/e4d53786e771cc6bdd55f180674b79f5b692e552/pkg/ingress/controller/launch.go#L252-L259
		// High enough QPS to fit all expected use cases. QPS=0 is not set here, because client code is overriding it.
		QPS: 1e6,
		// High enough Burst to fit all expected use cases. Burst=0 is not set here, because client code is overriding it.
		Burst:                       1e6,
		ResyncPeriod:                10 * time.Minute,
		MaxNumRequeues:              5,
		NumThreads:                  2,
		StdOut:                      out,
		StdErr:                      errOut,
		RestrictToOperatorNamespace: true,
	}
}

func (o *Options) AddGoFlags(fs *flag.FlagSet) {
	fs.Float64Var(&o.QPS, "qps", o.QPS, "The maximum QPS to the master from this client")
	fs.IntVar(&o.Burst, "burst", o.Burst, "The maximum burst for throttle")
	fs.DurationVar(&o.ResyncPeriod, "resync-period", o.ResyncPeriod, "If non-zero, will re-list this often. Otherwise, re-list will be delayed aslong as possible (until the upstream source closes the watch or times out.")

	fs.BoolVar(&o.RestrictToOperatorNamespace, "restrict-to-operator-namespace", o.RestrictToOperatorNamespace, "If true, operator will only handle Kubernetes objects in its own namespace.")
	fs.StringVar(&o.DBName, "db-name", o.DBName, "Database custom resource name")
}

func (o *Options) AddFlags(fs *pflag.FlagSet) {
	pfs := flag.NewFlagSet("labeler-server", flag.ExitOnError)
	o.AddGoFlags(pfs)
	fs.AddGoFlagSet(pfs)
}

func (o Options) WatchNamespace() string {
	if o.RestrictToOperatorNamespace {
		return o.LabelerNamespace
	}
	return corev1.NamespaceAll
}

func (o *Options) Apply(cfg *controller.Config) error {
	var err error

	cfg.ClientConfig.QPS = float32(o.QPS)
	cfg.ClientConfig.Burst = o.Burst
	cfg.ResyncPeriod = o.ResyncPeriod
	cfg.MaxNumRequeues = o.MaxNumRequeues
	cfg.NumThreads = o.NumThreads
	cfg.WatchNamespace = o.WatchNamespace()
	cfg.DBName = o.DBName

	if cfg.KubeClient, err = kubernetes.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}

	if cfg.DBClient, err = cs.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}
	cfg.KubeInformerFactory = informers.NewSharedInformerFactory(cfg.KubeClient, cfg.ResyncPeriod)

	return nil
}
