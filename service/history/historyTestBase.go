// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"

	log "github.com/sirupsen/logrus"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	testWorkflowClusterHosts = "127.0.0.1"
	testDatacenter           = ""
	testSchemaDir            = "../.."
)

type (
	// TestShardContext shard context for testing.
	TestShardContext struct {
		sync.RWMutex
		shardInfo              *persistence.ShardInfo
		transferSequenceNumber int64
		historyMgr             persistence.HistoryManager
		executionMgr           persistence.ExecutionManager
		domainCache            cache.DomainCache
		config                 *Config
		logger                 bark.Logger
		metricsClient          metrics.Client
		clusterMetadata        cluster.Metadata
	}

	// TestBase wraps the base setup needed to create workflows over engine layer.
	TestBase struct {
		persistence.TestBase
		ShardContext *TestShardContext
	}
)

var _ ShardContext = (*TestShardContext)(nil)

func newTestShardContext(shardInfo *persistence.ShardInfo, transferSequenceNumber int64, historyMgr persistence.HistoryManager,
	executionMgr persistence.ExecutionManager, domainCache cache.DomainCache, config *Config, logger bark.Logger) *TestShardContext {
	return &TestShardContext{
		shardInfo:              shardInfo,
		transferSequenceNumber: transferSequenceNumber,
		historyMgr:             historyMgr,
		executionMgr:           executionMgr,
		domainCache:            domainCache,
		config:                 config,
		logger:                 logger,
		metricsClient:          metrics.NewClient(tally.NoopScope, metrics.History),
		clusterMetadata:        cluster.GetTestClusterMetadata(false, false),
	}
}

// GetExecutionManager test implementation
func (s *TestShardContext) GetExecutionManager() persistence.ExecutionManager {
	return s.executionMgr
}

// GetHistoryManager test implementation
func (s *TestShardContext) GetHistoryManager() persistence.HistoryManager {
	return s.historyMgr
}

// GetDomainCache test implementation
func (s *TestShardContext) GetDomainCache() cache.DomainCache {
	return s.domainCache
}

// GetNextTransferTaskID test implementation
func (s *TestShardContext) GetNextTransferTaskID() (int64, error) {
	return atomic.AddInt64(&s.transferSequenceNumber, 1), nil
}

// GetTransferMaxReadLevel test implementation
func (s *TestShardContext) GetTransferMaxReadLevel() int64 {
	return atomic.LoadInt64(&s.transferSequenceNumber)
}

// GetTransferAckLevel test implementation
func (s *TestShardContext) GetTransferAckLevel() int64 {
	s.RLock()
	defer s.RUnlock()

	// TODO change make this cluster input parameter
	cluster := s.clusterMetadata.GetCurrentClusterName()
	// if can find corresponding ack level in the cluster to timer ack level map
	if ackLevel, ok := s.shardInfo.ClusterTransferAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	return s.shardInfo.TransferAckLevel
}

// UpdateTransferAckLevel test implementation
func (s *TestShardContext) UpdateTransferAckLevel(ackLevel int64) error {
	s.RLock()
	defer s.RUnlock()

	// TODO change make this cluster input parameter
	cluster := s.clusterMetadata.GetCurrentClusterName()
	if cluster == s.clusterMetadata.GetCurrentClusterName() {
		s.shardInfo.TransferAckLevel = ackLevel
	}
	s.shardInfo.ClusterTransferAckLevel[cluster] = ackLevel
	return nil
}

// GetTransferSequenceNumber test implementation
func (s *TestShardContext) GetTransferSequenceNumber() int64 {
	return atomic.LoadInt64(&s.transferSequenceNumber)
}

// GetTimerAckLevel test implementation
func (s *TestShardContext) GetTimerAckLevel(cluster string) time.Time {
	s.RLock()
	defer s.RUnlock()

	// if can find corresponding ack level in the cluster to timer ack level map
	if ackLevel, ok := s.shardInfo.ClusterTimerAckLevel[cluster]; ok {
		return ackLevel
	}
	// otherwise, default to existing ack level, which belongs to local cluster
	return s.shardInfo.TimerAckLevel
}

// UpdateTimerAckLevel test implementation
func (s *TestShardContext) UpdateTimerAckLevel(cluster string, ackLevel time.Time) error {
	s.RLock()
	defer s.RUnlock()

	if cluster == s.clusterMetadata.GetCurrentClusterName() {
		s.shardInfo.TimerAckLevel = ackLevel
	}
	s.shardInfo.ClusterTimerAckLevel[cluster] = ackLevel
	return nil
}

// CreateWorkflowExecution test implementation
func (s *TestShardContext) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (
	*persistence.CreateWorkflowExecutionResponse, error) {
	return s.executionMgr.CreateWorkflowExecution(request)
}

// UpdateWorkflowExecution test implementation
func (s *TestShardContext) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
	// assign IDs for the timer tasks. They need to be assigned under shard lock.
	for _, task := range request.TimerTasks {
		seqID, err := s.GetNextTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(seqID)
		s.logger.Infof("%v: TestShardContext: Assigning timer (timestamp: %v, seq: %v)",
			time.Now().UTC(), persistence.GetVisibilityTSFrom(task).UTC(), task.GetTaskID())
	}
	return s.executionMgr.UpdateWorkflowExecution(request)
}

// AppendHistoryEvents test implementation
func (s *TestShardContext) AppendHistoryEvents(request *persistence.AppendHistoryEventsRequest) error {
	return s.historyMgr.AppendHistoryEvents(request)
}

// NotifyNewHistoryEvent test implementation
func (s *TestShardContext) NotifyNewHistoryEvent(event *historyEventNotification) error {
	return nil
}

// GetConfig test implementation
func (s *TestShardContext) GetConfig() *Config {
	return s.config
}

// GetLogger test implementation
func (s *TestShardContext) GetLogger() bark.Logger {
	return s.logger
}

// GetMetricsClient test implementation
func (s *TestShardContext) GetMetricsClient() metrics.Client {
	return s.metricsClient
}

// Reset test implementation
func (s *TestShardContext) Reset() {
	atomic.StoreInt64(&s.shardInfo.RangeID, 0)
	atomic.StoreInt64(&s.shardInfo.TransferAckLevel, 0)
}

// GetRangeID test implementation
func (s *TestShardContext) GetRangeID() int64 {
	return atomic.LoadInt64(&s.shardInfo.RangeID)
}

// GetTimeSource test implementation
func (s *TestShardContext) GetTimeSource() common.TimeSource {
	return common.NewRealTimeSource()
}

// SetupWorkflowStoreWithOptions to setup workflow test base
func (s *TestBase) SetupWorkflowStoreWithOptions(options persistence.TestBaseOptions) {
	s.TestBase.SetupWorkflowStoreWithOptions(options)
	log := bark.NewLoggerFromLogrus(log.New())
	config := NewConfig(dynamicconfig.NewNopCollection(), 1)
	domainCache := cache.NewDomainCache(s.MetadataManager, log)
	s.ShardContext = newTestShardContext(s.ShardInfo, 0, s.HistoryMgr, s.WorkflowMgr, domainCache, config, log)
	s.TestBase.TaskIDGenerator = s.ShardContext
}

// SetupWorkflowStore to setup workflow test base
func (s *TestBase) SetupWorkflowStore() {
	s.TestBase.SetupWorkflowStore()
	log := bark.NewLoggerFromLogrus(log.New())
	config := NewConfig(dynamicconfig.NewNopCollection(), 1)
	domainCache := cache.NewDomainCache(s.MetadataManager, log)
	s.ShardContext = newTestShardContext(s.ShardInfo, 0, s.HistoryMgr, s.WorkflowMgr, domainCache, config, log)
	s.TestBase.TaskIDGenerator = s.ShardContext
}
