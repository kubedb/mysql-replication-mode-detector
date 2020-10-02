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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/go-sql-driver/mysql"
	sql_driver "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	TLSValueCustom     = "custom"
	TLSValueSkipVerify = "skip-verify"
)

func (c *Controller) queryInMySQLDatabase(podMeta metav1.ObjectMeta) ([]map[string]string, error) {
	// MySQL query to check master
	query := `SELECT MEMBER_HOST FROM performance_schema.replication_group_members
	INNER JOIN performance_schema.global_status ON (MEMBER_ID = VARIABLE_VALUE)
	WHERE VARIABLE_NAME='group_replication_primary_member';`

	en, err := c.getMySQLClient(podMeta)
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

func (c *Controller) getMySQLClient(podMeta metav1.ObjectMeta) (*xorm.Engine, error) {
	user, ok := os.LookupEnv(api.MySQLRootUserName)
	if !ok {
		return nil, fmt.Errorf("missing value of %v variable in MySQL Pod %v/%v", api.MySQLRootUserName, podMeta.Namespace, podMeta.Name)
	}
	password, ok := os.LookupEnv(api.MySQLRootPassword)
	if !ok {
		return nil, fmt.Errorf("missing value of %v variable in MySQL Pod %v/%v", api.MySQLRootPassword, podMeta.Namespace, podMeta.Name)
	}

	// MySQL CR name have passed by flag. we can use to get MySQL CR
	my, err := c.dbClient.KubedbV1alpha1().MySQLs(podMeta.Namespace).Get(context.TODO(), c.mysqlname, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	tlsConfig := ""
	if my.Spec.TLS != nil {
		serverSecret, err := c.kubeClient.CoreV1().Secrets(podMeta.Namespace).Get(context.TODO(), my.MustCertSecretName(api.MySQLServerCert), metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		cacrt := serverSecret.Data["ca.crt"]
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(cacrt)

		// tls custom setup
		if my.Spec.RequireSSL {
			err = sql_driver.RegisterTLSConfig(TLSValueCustom, &tls.Config{
				RootCAs: certPool,
			})
			if err != nil {
				return nil, err
			}
			tlsConfig = fmt.Sprintf("%s=%s", "tls", TLSValueCustom)
		} else {
			tlsConfig = fmt.Sprintf("%s=%s", "tls", TLSValueSkipVerify)
		}
	}
	cnnstr := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, password, api.LocalHost, api.MySQLNodePort, api.ResourceSingularMySQL, tlsConfig)
	en, err := xorm.NewEngine("mysql", cnnstr)
	en.ShowSQL()
	return en, err
}

func (c *Controller) eventuallyConnectWithMySQL(en *xorm.Engine) error {
	return wait.PollImmediate(5*time.Second, 5*time.Minute, func() (bool, error) {
		if err := en.Ping(); err != nil {
			return false, nil // don't return error. we need to retry.
		}
		return true, nil
	})
}
