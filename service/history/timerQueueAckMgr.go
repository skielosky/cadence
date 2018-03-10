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
	"math"
	"sort"
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

var (
	timerQueueAckMgrMaxTimestamp = time.Unix(0, math.MaxInt64)
)

type (
	timerTaskIDs []TimerSequenceID

	timerQueueAckMgr struct {
		clusterMetadata cluster.Metadata

		sync.Mutex
		shard                   ShardContext
		executionMgr            persistence.ExecutionManager
		logger                  bark.Logger
		currentCluster          string
		clusterOutstandingTasks map[string]map[TimerSequenceID]bool
		clusterReadLevel        map[string]TimerSequenceID
		clusterAckLevel         map[string]time.Time
		metricsClient           metrics.Client
		lastUpdated             time.Time
		config                  *Config
	}
	// for each cluster, the ack level is the point in time when
	// all timers before the ack level are processed.
	// for each cluster, the read level is the point in time when
	// all timers from ack level to read level are loaded in memory.

	// TODO this processing logic potentially has bug, refer to #605, #608
)

// Len implements sort.Interace
func (t timerTaskIDs) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t timerTaskIDs) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timerTaskIDs) Less(i, j int) bool {
	return compareTimerIDLess(&t[i], &t[j])
}

func newTimerQueueAckMgr(clusterMetadata cluster.Metadata, shard ShardContext, metricsClient metrics.Client,
	executionMgr persistence.ExecutionManager, logger bark.Logger) *timerQueueAckMgr {
	config := shard.GetConfig()
	timerQueueAckMgr := &timerQueueAckMgr{
		clusterMetadata:         clusterMetadata,
		shard:                   shard,
		executionMgr:            executionMgr,
		currentCluster:          clusterMetadata.GetCurrentClusterName(),
		clusterOutstandingTasks: make(map[string]map[TimerSequenceID]bool),
		clusterReadLevel:        make(map[string]TimerSequenceID),
		clusterAckLevel:         make(map[string]time.Time),
		metricsClient:           metricsClient,
		logger:                  logger,
		lastUpdated:             time.Now(),
		config:                  config,
	}
	// initialize all cluster read level to initial ack level, provided by shard
	for cluster := range clusterMetadata.GetAllClusterNames() {
		ackLevel := shard.GetTimerAckLevel(cluster)
		timerQueueAckMgr.clusterOutstandingTasks[cluster] = make(map[TimerSequenceID]bool)
		timerQueueAckMgr.clusterReadLevel[cluster] = TimerSequenceID{VisibilityTimestamp: ackLevel}
		timerQueueAckMgr.clusterAckLevel[cluster] = ackLevel
	}

	return timerQueueAckMgr
}

func (t *timerQueueAckMgr) readTimerTasks(clusterName string) ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error) {
	t.Lock()
	readLevel, ok := t.clusterReadLevel[clusterName]
	if !ok {
		t.panicUnknownCluster(clusterName)
	}
	t.Unlock()

	tasks, token, err := t.getTimerTasks(readLevel.VisibilityTimestamp, timerQueueAckMgrMaxTimestamp, t.config.TimerTaskBatchSize)
	if err != nil {
		return nil, nil, false, err
	}

	t.logger.Debugf("readTimerTasks: ReadLevel: (%s) count: %v, next token: %v", readLevel, len(tasks), token)

	// We filter tasks so read only moves to desired timer tasks.
	// We also get a look ahead task but it doesn't move the read level, this is for timer
	// to wait on it instead of doing queries.

	var lookAheadTask *persistence.TimerTaskInfo
	filteredTasks := []*persistence.TimerTaskInfo{}

	t.Lock()
	defer t.Unlock()
	// since we have already checked that the clusterName is a valid key of clusterReadLevel
	// there shall be no validation
	readLevel = t.clusterReadLevel[clusterName]
	outstandingTasks := t.clusterOutstandingTasks[clusterName]
TaskFilterLoop:
	for _, task := range tasks {
		taskSeq := TimerSequenceID{VisibilityTimestamp: task.VisibilityTimestamp, TaskID: task.TaskID}
		if _, ok := outstandingTasks[taskSeq]; ok {
			t.logger.Infof("Skipping task: %v.  WorkflowID: %v, RunID: %v, Type: %v", taskSeq.String(), task.WorkflowID,
				task.RunID, task.TaskType)
			continue TaskFilterLoop
		}

		if task.VisibilityTimestamp.Before(readLevel.VisibilityTimestamp) {
			t.logger.Fatalf(
				"Next timer task time stamp is less than current timer task read level. timer task: (%s), ReadLevel: (%s)",
				taskSeq, readLevel)
		}

		if !t.isProcessNow(task.VisibilityTimestamp) {
			lookAheadTask = task
			break TaskFilterLoop
		}

		t.logger.Debugf("Moving timer read level: (%s)", taskSeq)
		readLevel = taskSeq
		outstandingTasks[taskSeq] = false
		filteredTasks = append(filteredTasks, task)
	}
	t.clusterReadLevel[clusterName] = readLevel

	// We may have large number of timers which need to be fired immediately.  Return true in such case so the pump
	// can call back immediately to retrieve more tasks
	moreTasks := lookAheadTask == nil && len(token) != 0

	return filteredTasks, lookAheadTask, moreTasks, nil
}

func (t *timerQueueAckMgr) completeTimerTask(clusterName string, taskID TimerSequenceID) {
	t.Lock()
	outstandingTasks, ok := t.clusterOutstandingTasks[clusterName]
	if !ok {
		t.panicUnknownCluster(clusterName)
	}
	if _, ok = outstandingTasks[taskID]; ok {
		outstandingTasks[taskID] = true
	}
	t.Unlock()
}

func (t *timerQueueAckMgr) updateAckLevel(clusterName string) {
	t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateCounter)

	t.Lock()
	ackLevel, ok := t.clusterAckLevel[clusterName]
	if !ok {
		t.panicUnknownCluster(clusterName)
	}
	outstandingTasks := t.clusterOutstandingTasks[clusterName]
	initialAckLevel := ackLevel
	updatedAckLevel := ackLevel

	// Timer IDs can have holes in the middle. So we sort the map to get the order to
	// check. TODO: we can maintain a sorted slice as well.
	var timerTaskIDs timerTaskIDs
	for k := range outstandingTasks {
		timerTaskIDs = append(timerTaskIDs, k)
	}
	sort.Sort(timerTaskIDs)

MoveAckLevelLoop:
	for _, current := range timerTaskIDs {
		acked := outstandingTasks[current]
		if acked {
			updatedAckLevel = current.VisibilityTimestamp
			delete(outstandingTasks, current)
		} else {
			break MoveAckLevelLoop
		}
	}
	t.clusterAckLevel[clusterName] = updatedAckLevel
	t.Unlock()

	// Do not update Acklevel if nothing changed upto force update interval
	if initialAckLevel == updatedAckLevel && time.Since(t.lastUpdated) < t.config.TimerProcessorForceUpdateInterval {
		return
	}

	t.logger.Debugf("Updating timer ack level: %v", updatedAckLevel)

	// Always update ackLevel to detect if the shared is stolen
	if err := t.shard.UpdateTimerAckLevel(clusterName, updatedAckLevel); err != nil {
		t.metricsClient.IncCounter(metrics.TimerQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
		t.logger.Errorf("Error updating timer ack level for shard: %v", err)
	} else {
		t.lastUpdated = time.Now()
	}
}

func (t *timerQueueAckMgr) getTimerTasks(minTimestamp time.Time, maxTimestamp time.Time, batchSize int) ([]*persistence.TimerTaskInfo, []byte, error) {
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

	retryCount := t.config.TimerProcessorGetFailureRetryCount
	for attempt := 0; attempt < retryCount; attempt++ {
		response, err := t.executionMgr.GetTimerIndexTasks(request)
		if err == nil {
			return response.Timers, response.NextPageToken, nil
		}
		backoff := time.Duration(attempt * 100)
		time.Sleep(backoff * time.Millisecond)
	}
	return nil, nil, ErrMaxAttemptsExceeded
}

func (t *timerQueueAckMgr) isProcessNow(expiryTime time.Time) bool {
	return !expiryTime.IsZero() && expiryTime.UnixNano() <= time.Now().UnixNano()
}

func (t *timerQueueAckMgr) panicUnknownCluster(clusterName string) {
	t.logger.Fatalf("Cannot find target cluster: %v, all known clusters %v.", clusterName, t.clusterMetadata.GetAllClusterNames())
}
