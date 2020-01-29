/*
Copyright The KubeDB Authors.

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

package controller

import (
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"kmodules.xyz/client-go/tools/portforward"
)

func (lc *LabelController) queryInMySQLDatabase(objMeta metav1.ObjectMeta, user, password, query string) ([]map[string]string, error) {
	tunnel, err := lc.forwardPort(objMeta)
	if err != nil {
		return nil, err
	}
	defer tunnel.Close()

	en, err := lc.getMySQLClient(tunnel, user, password)
	if err != nil {
		return nil, err
	}
	defer en.Close()

	// connecting with MySQL database
	err = lc.eventuallyConnectWithMySQL(en)
	if err != nil {
		return nil, err
	}

	r, err := en.QueryString(query)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, fmt.Errorf("query result is nil")
	}
	return r, nil
}

func (lc *LabelController) forwardPort(objMeta metav1.ObjectMeta) (*portforward.Tunnel, error) {
	tunnel := portforward.NewTunnel(
		lc.kubeClient.CoreV1().RESTClient(),
		lc.clientConfig,
		objMeta.Namespace,
		objMeta.Name,
		3306,
	)

	if err := tunnel.ForwardPort(); err != nil {
		return nil, err
	}
	return tunnel, nil
}

func (lc *LabelController) getMySQLClient(tunnel *portforward.Tunnel, user, password string) (*xorm.Engine, error) {
	cnnstr := fmt.Sprintf("%v:%v@tcp(127.0.0.1:%v)/%s", user, password, tunnel.Local, DatabaseName)
	return xorm.NewEngine("mysql", cnnstr)
}

func (lc *LabelController) eventuallyConnectWithMySQL(en *xorm.Engine) error {
	return wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
		if err := en.Ping(); err != nil {
			return false, nil // don't return error. we need to retry.
		}
		return true, nil
	})
}
