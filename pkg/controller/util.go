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
	"fmt"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"k8s.io/apimachinery/pkg/util/wait"
)

func (c *Controller) queryInMySQLDatabase(user, password, query string) ([]map[string]string, error) {
	en, err := c.getMySQLClient(user, password)
	if err != nil {
		return nil, err
	}
	defer en.Close()

	// connecting with MySQL database
	err = c.eventuallyConnectWithMySQL(en)
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

func (c *Controller) getMySQLClient(user, password string) (*xorm.Engine, error) {
	cnnstr := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s", user, password, api.LocalHost, api.MySQLNodePort, DatabaseName)
	return xorm.NewEngine("mysql", cnnstr)
}

func (c *Controller) eventuallyConnectWithMySQL(en *xorm.Engine) error {
	return wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
		if err := en.Ping(); err != nil {
			return false, nil // don't return error. we need to retry.
		}
		return true, nil
	})
}
