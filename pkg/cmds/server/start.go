/*
Copyright AppsCode Inc. and Contributors

Licensed under the PolyForm Noncommercial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/PolyForm-Noncommercial-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"kubedb.dev/mysql-replication-mode-detector/pkg/controller"

	"kmodules.xyz/client-go/tools/clientcmd"
)

func (o Options) Validate(args []string) error {
	return nil
}

func (o *Options) Complete() error {
	return nil
}

func (o Options) labelerConfig() (*controller.Config, error) {
	config, err := clientcmd.BuildConfigFromContext("", "")
	if err != nil {
		return nil, err
	}

	labelerConfig := controller.Config{}
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
