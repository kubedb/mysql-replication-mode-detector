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
	"fmt"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mgoptions "go.mongodb.org/mongo-driver/mongo/options"
	"gomodules.xyz/x/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"kmodules.xyz/client-go/tools/certholder"
)

func (c *Controller) GetMongoDBConnectionURL(podMeta metav1.ObjectMeta, db *api.MongoDB) string {
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

	url := c.GetMongoDBConnectionURL(podMeta, db)

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

func (c *Controller) isMongoDBPrimary(podMeta metav1.ObjectMeta) (bool, error) {
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
