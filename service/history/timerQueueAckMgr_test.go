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
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	timerQueueAckMgrSuite struct {
		suite.Suite

		mockExecutionMgr *mocks.ExecutionManager
		mockShardMgr     *mocks.ShardManager
		mockMetadataMgr  *mocks.MetadataManager
		mockHistoryMgr   *mocks.HistoryManager
		mockShard        ShardContext
		clusterMetadata  cluster.Metadata
		metricsClient    metrics.Client
		logger           bark.Logger
		timerQueueAckMgr *timerQueueAckMgr
	}
)

func TestTimerQueueAckMgrSuite(t *testing.T) {
	s := new(timerQueueAckMgrSuite)
	suite.Run(t, s)
}

func (s *timerQueueAckMgrSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *timerQueueAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueAckMgrSuite) SetupTest() {
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShardMgr = &mocks.ShardManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterMetadata = cluster.GetTestClusterMetadata(false, false)
	s.mockShard = &shardContextImpl{
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardMgr,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewConfig(dynamicconfig.NewNopCollection(), 1),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.logger),
		metricsClient:             s.metricsClient,
		clusterMetadata:           s.clusterMetadata,
	}

	s.timerQueueAckMgr = newTimerQueueAckMgr(s.clusterMetadata, s.mockShard, s.metricsClient, s.mockExecutionMgr, s.logger)

}

func (s *timerQueueAckMgrSuite) TearDownTest() {

}

func (s *timerQueueAckMgrSuite) TestIsProcessNow() {
	timeBefore := time.Now().Add(-10 * time.Second)
	s.True(s.timerQueueAckMgr.isProcessNow(timeBefore))

	timeAfter := time.Now().Add(10 * time.Second)
	s.False(s.timerQueueAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{
			&persistence.TimerTaskInfo{
				DomainID:            "some random domain ID",
				WorkflowID:          "some random workflow ID",
				RunID:               "some random run ID",
				VisibilityTimestamp: time.Now().Add(-5 * time.Second),
				TaskID:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventID:             int64(28),
				ScheduleAttempt:     0,
			},
		},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Equal(response.NextPageToken, token)
}
