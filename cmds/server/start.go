package server

import (
	"kubedb.dev/mysql-primary-labeler/pkg/controller"

	"kmodules.xyz/client-go/tools/clientcmd"
)

func (o Options) Validate(args []string) error {
	return nil
}

func (o *Options) Complete() error {
	return nil
}

func (o Options) labelerConfig() (*controller.LabelerConfig, error) {
	config, err := clientcmd.BuildConfigFromContext("", "")
	if err != nil {
		return nil, err
	}

	labelerConfig := controller.LabelerConfig{}
	labelerConfig.ClientConfig = config

	if err := o.Apply(&labelerConfig); err != nil {
		return nil, err
	}

	return &labelerConfig, nil
}

func (o Options) RunLabeler(stopCh <-chan struct{}) error {
	config, err := o.labelerConfig()
	if err != nil {
		return err
	}

	lc, err := config.New()
	if err != nil {
		return err
	}

	lc.RunLabelController(stopCh)

	return nil
}
