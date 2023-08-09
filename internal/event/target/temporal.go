// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package target

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/minio/minio/internal/event"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/minio/internal/once"
	"github.com/minio/minio/internal/store"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	xnet "github.com/minio/pkg/net"
)

// Temporal constants
const (
	TemporalAddress    = "address"
	TemporalPassword   = "password" // not so sure if this is needed
	TemporalNamespace  = "namespace"
	TemporalTaskQueue  = "task_queue"
	TemporalWorkflow   = "workflow"
	TemporalQueueDir   = "queue_dir"
	TemporalQueueLimit = "queue_limit"

	TemporalTLS           = "tls"
	TemporalTLSSkipVerify = "tls_skip_verify"
	TemporalCertAuthority = "cert_authority"
	TemporalClientCert    = "client_cert"
	TemporalClientKey     = "client_key"

	EnvTemporalEnable        = "MINIO_NOTIFY_TEMPORAL_ENABLE"
	EnvTemporalAddress       = "MINIO_NOTIFY_TEMPORAL_ADDRESS"
	EnvTemporalPassword      = "MINIO_NOTIFY_TEMPORAL_PASSWORD"
	EnvTemporalNamespace     = "MINIO_NOTIFY_TEMPORAL_NAMESPACE"
	EnvTemporalTaskQueue     = "MINIO_NOTIFY_TEMPORAL_TASK_QUEUE"
	EnvTemporalWorkflow      = "MINIO_NOTIFY_TEMPORAL_WORKFLOW"
	EnvTemporalQueueDir      = "MINIO_NOTIFY_TEMPORAL_QUEUE_DIR"
	EnvTemporalQueueLimit    = "MINIO_NOTIFY_TEMPORAL_QUEUE_LIMIT"
	EnvTemporalTLS           = "MINIO_NOTIFY_TEMPORAL_TLS"
	EnvTemporalTLSSkipVerify = "MINIO_NOTIFY_TEMPORAL_TLS_SKIP_VERIFY"
	EnvTemporalCertAuthority = "MINIO_NOTIFY_TEMPORAL_CERT_AUTHORITY"
	EnvTemporalClientCert    = "MINIO_NOTIFY_TEMPORAL_CLIENT_CERT"
	EnvTemporalClientKey     = "MINIO_NOTIFY_TEMPORAL_CLIENT_KEY"
)

// TemporalArgs - Temporal target arguments.
type TemporalArgs struct {
	Enable     bool      `json:"enable"`
	Addr       xnet.Host `json:"address"`
	Password   string    `json:"password"`
	Namespace  string    `json:"namespace"`
	TaskQueue  string    `json:"taskQueue"`
	Workflow   string    `json:"workflow"`
	QueueDir   string    `json:"queueDir"`
	QueueLimit uint64    `json:"queueLimit"`

	// Connection related fields.
	TLS           bool   `json:"tls"`
	TLSSkipVerify bool   `json:"tlsSkipVerify"`
	Secure        bool   `json:"secure"`
	CertAuthority string `json:"certAuthority"`
	ClientCert    string `json:"clientCert"`
	ClientKey     string `json:"clientKey"`

	RootCAs *x509.CertPool `json:"-"`
}

// TemporalAccessEvent holds event log data and timestamp
type TemporalAccessEvent struct {
	Event     []event.Event
	EventTime string
}

// Validate RedisArgs fields
func (t TemporalArgs) Validate() error {
	if !t.Enable {
		return nil
	}

	if t.Addr.IsEmpty() {
		return fmt.Errorf("empty temporal host address")
	}

	if t.TaskQueue == "" {
		return fmt.Errorf("empty task queue name")
	}

	if t.Workflow == "" {
		return fmt.Errorf("empty workflow name")
	}

	if t.ClientCert != "" && t.ClientKey == "" || t.ClientCert == "" && t.ClientKey != "" {
		return fmt.Errorf("cert and key must be specified as a pair")
	}

	if t.QueueDir != "" {
		if !filepath.IsAbs(t.QueueDir) {
			return fmt.Errorf("queueDir path should be absolute")
		}
	}

	return nil
}

func (t TemporalArgs) connectTemporal() (client.Client, error) {
	options := client.Options{
		HostPort:  t.Addr.String(),
		Namespace: "default",
	}

	if t.Namespace != "" {
		options.Namespace = t.Namespace
	}

	if t.Secure || t.TLS && t.TLSSkipVerify {
		options.ConnectionOptions.TLS.InsecureSkipVerify = true
		options.ConnectionOptions.TLS.ServerName = t.Addr.String()
	} else if t.TLS {
		options.ConnectionOptions.TLS.InsecureSkipVerify = false
	}

	if t.CertAuthority != "" {
		var serverCAPool *x509.CertPool
		if t.CertAuthority != "" {
			serverCAPool = x509.NewCertPool()
			b, err := os.ReadFile(t.CertAuthority)
			if err != nil {
				return nil, fmt.Errorf("failed reading server CA: %w", err)
			} else if !serverCAPool.AppendCertsFromPEM(b) {
				return nil, fmt.Errorf("server CA PEM file invalid")
			}
		}
		options.ConnectionOptions.TLS.RootCAs = serverCAPool
	}

	if t.ClientCert != "" && t.ClientKey != "" {
		cert, err := tls.LoadX509KeyPair(t.ClientCert, t.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed loading client cert and key: %w", err)
		}
		options.ConnectionOptions.TLS.Certificates = []tls.Certificate{cert}
	}
	return client.NewClient(options)
}

// TemporalTarget - Temporal target.
type TemporalTarget struct {
	initOnce once.Init

	id             event.TargetID
	args           TemporalArgs
	temporalClient client.Client
	store          store.Store[event.Event]
	firstPing      bool
	loggerOnce     logger.LogOnce
	quitCh         chan struct{}
}

// ID - returns target ID.
func (target *TemporalTarget) ID() event.TargetID {
	return target.id
}

// Name - returns the Name of the target.
func (target *TemporalTarget) Name() string {
	return target.ID().String()
}

// Store returns any underlying store if set.
func (target *TemporalTarget) Store() event.TargetStore {
	return target.store
}

// IsActive - Return true if target is up and active
func (target *TemporalTarget) IsActive() (bool, error) {
	if err := target.init(); err != nil {
		return false, err
	}
	return target.isActive()
}

func (target *TemporalTarget) isActive() (bool, error) {
	if target.temporalClient == nil {
		client, err := target.args.connectTemporal()
		if err != nil {
			target.loggerOnce(context.Background(), err, target.ID().String())
			return false, err
		}
		target.temporalClient = client
	}

	_, err := target.temporalClient.WorkflowService().GetClusterInfo(context.Background(), &workflowservice.GetClusterInfoRequest{})
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return false, err
	}
	return true, nil
}

// Save - saves the events to the store if questore is configured, which will be replayed when the Temporal connection is active.
func (target *TemporalTarget) Save(eventData event.Event) error {
	if target.store != nil {
		return target.store.Put(eventData)
	}
	if err := target.init(); err != nil {
		return err
	}
	_, err := target.isActive()
	if err != nil {
		return err
	}
	return target.send(eventData)
}

// send - sends an event to the redis.
func (target *TemporalTarget) send(eventData event.Event) error {
	objectName, err := url.QueryUnescape(eventData.S3.Object.Key)
	if err != nil {
		return err
	}
	key := eventData.S3.Bucket.Name + "/" + objectName

	data, err := json.Marshal(event.Log{EventName: eventData.EventName, Key: key, Records: []event.Event{eventData}})
	if err != nil {
		return err
	}

	workflowOptions := client.StartWorkflowOptions{
		TaskQueue: target.args.TaskQueue,
	}

	_, err = target.temporalClient.ExecuteWorkflow(context.Background(), workflowOptions, target.args.Workflow, data)
	return err
}

// SendFromStore - reads an event from store and sends it to a temporal task queue.
func (target *TemporalTarget) SendFromStore(eventKey string) error {
	if err := target.init(); err != nil {
		return err
	}

	eventData, eErr := target.store.Get(eventKey)
	if eErr != nil {
		// The last event key in a successful batch will be sent in the channel atmost once by the replayEvents()
		// Such events will not exist and would've been already been sent successfully.
		if os.IsNotExist(eErr) {
			return nil
		}
		return eErr
	}

	if err := target.send(eventData); err != nil {
		if xnet.IsConnRefusedErr(err) {
			return store.ErrNotConnected
		}
		return err
	}

	// Delete the event from store.
	return target.store.Del(eventKey)
}

// Close - releases the resources used by the pool.
func (target *TemporalTarget) Close() error {
	close(target.quitCh)
	target.temporalClient.Close()
	return nil
}

func (target *TemporalTarget) init() error {
	return target.initOnce.Do(target.initTemporal)
}

func (target *TemporalTarget) initTemporal() error {
	target.firstPing = true

	client, err := target.args.connectTemporal()
	if err != nil {
		target.loggerOnce(context.Background(), err, target.ID().String())
		return err
	}
	target.temporalClient = client

	yes, err := target.isActive()
	if err != nil {
		return err
	}
	if !yes {
		return store.ErrNotConnected
	}

	return nil
}

// NewTemporalTarget - creates new Temporal target.
func NewTemporalTarget(id string, args TemporalArgs, loggerOnce logger.LogOnce) (*TemporalTarget, error) {
	var queueStore store.Store[event.Event]
	if args.QueueDir != "" {
		queueDir := filepath.Join(args.QueueDir, storePrefix+"-temporal-"+id)
		queueStore = store.NewQueueStore[event.Event](queueDir, args.QueueLimit, event.StoreExtension)
		if err := queueStore.Open(); err != nil {
			return nil, fmt.Errorf("unable to initialize the queue store of Temporal `%s`: %w", id, err)
		}
	}

	target := &TemporalTarget{
		id:         event.TargetID{ID: id, Name: "temporal"},
		args:       args,
		store:      queueStore,
		loggerOnce: loggerOnce,
		quitCh:     make(chan struct{}),
	}

	if target.store != nil {
		store.StreamItems(target.store, target, target.quitCh, target.loggerOnce)
	}

	return target, nil
}
