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
	"strings"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	_ "github.com/go-sql-driver/mysql"
	sql_driver "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mgoptions "go.mongodb.org/mongo-driver/mongo/options"
	"gomodules.xyz/x/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/apis/core"
	"kmodules.xyz/client-go/tools/certholder"
)

func (c *Controller) queryInMySQLDatabase(podMeta metav1.ObjectMeta) ([]map[string]string, error) {
	// MySQL query to check master
	// For version `8.*.*` the primary member information presents in `performance_schema.replication_group_members` table.
	// However, it does not exit in `5.*.*`. The primary member information for `5.*.*` can be found in `performance_schema.global_status` table.
	// Hence, we are joining both table so that the query works for the both versions.
	query := `SELECT MEMBER_HOST FROM performance_schema.replication_group_members
	INNER JOIN performance_schema.global_status ON (MEMBER_ID = VARIABLE_VALUE)
	WHERE VARIABLE_NAME="group_replication_primary_member" AND MEMBER_STATE="ONLINE";`

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
	my, err := c.dbClient.KubedbV1alpha2().MySQLs(podMeta.Namespace).Get(context.TODO(), c.dbName, metav1.GetOptions{})
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
			err = sql_driver.RegisterTLSConfig(api.MySQLTLSConfigCustom, &tls.Config{
				RootCAs: certPool,
			})
			if err != nil {
				return nil, err
			}
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigCustom)
		} else {
			tlsConfig = fmt.Sprintf("tls=%s", api.MySQLTLSConfigSkipVerify)
		}
	}
	cnnstr := fmt.Sprintf("%v:%v@tcp(%s:%d)/%s?%s", user, password, api.LocalHost, api.MySQLDatabasePort, api.ResourceSingularMySQL, tlsConfig)
	en, err := xorm.NewEngine("mysql", cnnstr)
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

func (c *Controller) GetURL(podMeta metav1.ObjectMeta, db *api.MongoDB) string {
	clientPodName := podMeta.Name
	nodeType := clientPodName[:strings.LastIndex(clientPodName, "-")]
	return fmt.Sprintf("%s.%s.%s.svc", clientPodName, db.GoverningServiceName(nodeType), podMeta.Namespace)
}
func (c *Controller) GetMongoDBRootCredentials(db *api.MongoDB) (string, string, error) {
	secret, err := c.kubeClient.CoreV1().Secrets(db.Namespace).Get(context.TODO(), db.Spec.AuthSecret.Name, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}
	return string(secret.Data[core.BasicAuthUsernameKey]), string(secret.Data[core.BasicAuthPasswordKey]), nil
}

func (c *Controller) GetMongoDBClientOpts(url string, db *api.MongoDB) (*mgoptions.ClientOptions, error) {
	var clientOpts *mgoptions.ClientOptions
	if db.Spec.SSLMode == api.SSLModeRequireSSL {
		secretName := db.MustCertSecretName(api.MongoDBClientCert, "")
		certSecret, err := c.kubeClient.CoreV1().Secrets(db.Namespace).Get(context.TODO(), secretName, metav1.GetOptions{})
		if err != nil {
			log.Error(err, "failed to get certificate secret", "Secret", secretName)
			return nil, err
		}

		certs, _ := certholder.DefaultHolder.
			ForResource(api.SchemeGroupVersion.WithResource(api.ResourcePluralMongoDB), db.ObjectMeta)
		_, err = certs.Save(certSecret)
		if err != nil {
			log.Error(err, "failed to save certificate")
			return nil, err
		}

		paths, err := certs.Get(db.MustCertSecretName(api.MongoDBClientCert, ""))
		if err != nil {
			return nil, err
		}

		uri := fmt.Sprintf("mongodb://%s/admin?tls=true&authMechanism=MONGODB-X509&tlsCAFile=%v&tlsCertificateKeyFile=%v", url, paths.CACert, paths.Pem)
		clientOpts = mgoptions.Client().ApplyURI(uri)
	} else {
		user, pass, err := c.GetMongoDBRootCredentials(db)
		if err != nil {
			return nil, err
		}

		clientOpts = mgoptions.Client().ApplyURI(fmt.Sprintf("mongodb://%s:%s@%s", user, pass, url))
	}

	clientOpts.SetDirect(true)

	return clientOpts, nil
}

func (c *Controller) GetMongoClient(podMeta metav1.ObjectMeta) (*mongo.Client, error) {
	db, err := c.dbClient.KubedbV1alpha2().MongoDBs(podMeta.Namespace).Get(context.TODO(), c.dbName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	url := c.GetURL(podMeta, db)

	clientOpts, err := c.GetMongoDBClientOpts(url, db)
	if err != nil {
		return nil, err
	}

	client, err := mongo.Connect(context.Background(), clientOpts)
	if err != nil {
		return nil, err
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (c *Controller) IsMaster(podMeta metav1.ObjectMeta) (bool, error) {
	client, err := c.GetMongoClient(podMeta)
	if err != nil {
		return false, err
	}

	res := make(map[string]interface{})

	err = client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "isMaster", Value: "1"}}).Decode(&res)
	if err != nil {
		return false, err
	}

	if val, ok := res["ismaster"]; ok && val == true {
		return true, nil
	}
	return false, nil
}
