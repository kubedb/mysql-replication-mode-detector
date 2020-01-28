package server

import (
	"flag"
	"io"
	"time"

	cs "kubedb.dev/apimachinery/client/clientset/versioned"
	"kubedb.dev/mysql-primary-labeler/pkg/controller"

	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
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
		RestrictToOperatorNamespace: false,
	}
}

func (s *Options) AddGoFlags(fs *flag.FlagSet) {
	fs.Float64Var(&s.QPS, "qps", s.QPS, "The maximum QPS to the master from this client")
	fs.IntVar(&s.Burst, "burst", s.Burst, "The maximum burst for throttle")
	fs.DurationVar(&s.ResyncPeriod, "resync-period", s.ResyncPeriod, "If non-zero, will re-list this often. Otherwise, re-list will be delayed aslong as possible (until the upstream source closes the watch or times out.")

	fs.BoolVar(&s.RestrictToOperatorNamespace, "restrict-to-operator-namespace", s.RestrictToOperatorNamespace, "If true, operator will only handle Kubernetes objects in its own namespace.")
}

func (s *Options) AddFlags(fs *pflag.FlagSet) {
	pfs := flag.NewFlagSet("labeler-server", flag.ExitOnError)
	s.AddGoFlags(pfs)
	fs.AddGoFlagSet(pfs)
}

func (s Options) WatchNamespace() string {
	if s.RestrictToOperatorNamespace {
		return s.LabelerNamespace
	}
	return v1.NamespaceAll
}

func (s *Options) Apply(cfg *controller.LabelerConfig) error {
	var err error

	cfg.ClientConfig.QPS = float32(s.QPS)
	cfg.ClientConfig.Burst = s.Burst
	cfg.ResyncPeriod = s.ResyncPeriod
	cfg.MaxNumRequeues = s.MaxNumRequeues
	cfg.NumThreads = s.NumThreads
	cfg.WatchNamespace = s.WatchNamespace()

	if cfg.KubeClient, err = kubernetes.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}

	if cfg.DBClient, err = cs.NewForConfig(cfg.ClientConfig); err != nil {
		return err
	}
	cfg.KubeInformerFactory = informers.NewSharedInformerFactory(cfg.KubeClient, cfg.ResyncPeriod)

	return nil
}
