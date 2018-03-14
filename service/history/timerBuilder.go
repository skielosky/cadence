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
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	w "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

// Timer task status
const (
	TimerTaskStatusNone = iota
	TimerTaskStatusCreated
)

// Activity Timer task status
const (
	TimerTaskStatusCreatedStartToClose = 1 << iota
	TimerTaskStatusCreatedScheduleToStart
	TimerTaskStatusCreatedScheduleToClose
	TimerTaskStatusCreatedHeartbeat
)

type (
	timerDetails struct {
		TimerSequenceID TimerSequenceID
		TaskCreated     bool
		TimerID         string
		ActivityID      int64
		TimeoutType     w.TimeoutType
		EventID         int64
		TimeoutSec      int32
	}

	timers []*timerDetails

	timerBuilder struct {
		userTimers             timers                            // all user timers sorted by expiry time stamp.
		pendingUserTimers      map[string]*persistence.TimerInfo // all user timers indexed by timerID(this just points to mutable state)
		isLoadedUserTimers     bool
		activityTimers         timers
		pendingActivityTimers  map[int64]*persistence.ActivityInfo
		isLoadedActivityTimers bool
		config                 *Config
		logger                 bark.Logger
		localSeqNumGen         SequenceNumberGenerator // This one used to order in-memory list.
		timeSource             common.TimeSource
	}

	// TimerSequenceID - Visibility timer stamp + Sequence Number.
	TimerSequenceID struct {
		VisibilityTimestamp time.Time
		TaskID              int64
	}

	// SequenceNumberGenerator - Generates next sequence number.
	SequenceNumberGenerator interface {
		NextSeq() int64
	}

	localSeqNumGenerator struct {
		counter int64
	}
)

func (s TimerSequenceID) String() string {
	return fmt.Sprintf("timestamp: %v, seq: %v", s.VisibilityTimestamp.UTC(), s.TaskID)
}

// Len implements sort.Interace
func (t timers) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
// Swap implements sort.Interface.
func (t timers) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t timers) Less(i, j int) bool {
	return compareTimerIDLess(&t[i].TimerSequenceID, &t[j].TimerSequenceID)
}

func (td *timerDetails) String() string {
	return fmt.Sprintf("TimerDetails: SeqID: %s, TimerID: %v, ActivityID: %v, TaskCreated: %v, EventID: %v, TimeoutType: %v, TimeoutSec: %v",
		td.TimerSequenceID, td.TimerID, td.ActivityID, td.TaskCreated, td.EventID, td.TimeoutType.String(), td.TimeoutSec)
}

func (l *localSeqNumGenerator) NextSeq() int64 {
	return atomic.AddInt64(&l.counter, 1)
}

// newTimerBuilder creates a timer builder.
func newTimerBuilder(config *Config, logger bark.Logger, timeSource common.TimeSource) *timerBuilder {
	return &timerBuilder{
		userTimers:            timers{},
		pendingUserTimers:     make(map[string]*persistence.TimerInfo),
		activityTimers:        timers{},
		pendingActivityTimers: make(map[int64]*persistence.ActivityInfo),
		config:                config,
		logger:                logger.WithField(logging.TagWorkflowComponent, "timer-builder"),
		localSeqNumGen:        &localSeqNumGenerator{counter: 1},
		timeSource:            timeSource,
	}
}

// AddDecisionTimeoutTask - Add a decision timeout task.
func (tb *timerBuilder) AddDecisionTimoutTask(scheduleID, scheduleAttempt int64,
	startToCloseTimeout int32) *persistence.DecisionTimeoutTask {
	timeOutTask := tb.createDecisionTimeoutTask(startToCloseTimeout, scheduleID, scheduleAttempt,
		w.TimeoutTypeStartToClose)
	tb.logger.Debugf("Adding Decision Timeout: with timeout: %v sec, EventID: %v",
		startToCloseTimeout, timeOutTask.EventID)
	return timeOutTask
}

// AddDecisionScheduleToStartTimoutTask - Add a decision schedule to start timeout task.
func (tb *timerBuilder) AddScheduleToStartDecisionTimoutTask(scheduleID, scheduleAttempt int64,
	scheduleToStartTimeout int32) *persistence.DecisionTimeoutTask {
	timeOutTask := tb.createDecisionTimeoutTask(scheduleToStartTimeout, scheduleID, scheduleAttempt,
		w.TimeoutTypeScheduleToStart)
	tb.logger.Debugf("Adding Decision ScheduleToStartTimeout: with timeout: %v sec, EventID: %v",
		scheduleToStartTimeout, timeOutTask.EventID)
	return timeOutTask
}

func (tb *timerBuilder) AddScheduleToCloseActivityTimeout(
	ai *persistence.ActivityInfo) (*persistence.ActivityTimeoutTask, error) {
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutTypeScheduleToClose, ai.ScheduleToCloseTimeout, nil), nil
}

func (tb *timerBuilder) AddStartToCloseActivityTimeout(ai *persistence.ActivityInfo) (*persistence.ActivityTimeoutTask,
	error) {
	return tb.AddActivityTimeoutTask(ai.ScheduleID, w.TimeoutTypeStartToClose, ai.StartToCloseTimeout, nil), nil
}

// AddActivityTimeoutTask - Adds an activity timeout task.
func (tb *timerBuilder) AddActivityTimeoutTask(scheduleID int64,
	timeoutType w.TimeoutType, fireTimeout int32, baseTime *time.Time) *persistence.ActivityTimeoutTask {
	if fireTimeout <= 0 {
		return nil
	}

	timeOutTask := tb.createActivityTimeoutTask(fireTimeout, timeoutType, scheduleID, baseTime)
	tb.logger.Debugf("%s: Adding Activity Timeout: with timeout: %v sec, TimeoutType: %v, EventID: %v",
		time.Now(), fireTimeout, timeoutType.String(), timeOutTask.EventID)
	return timeOutTask
}

// AddUserTimer - Adds an user timeout request.
func (tb *timerBuilder) AddUserTimer(ti *persistence.TimerInfo, msBuilder *mutableStateBuilder) {
	if !tb.isLoadedUserTimers {
		tb.loadUserTimers(msBuilder)
	}
	seqNum := tb.localSeqNumGen.NextSeq()
	timer := &timerDetails{
		TimerSequenceID: TimerSequenceID{VisibilityTimestamp: ti.ExpiryTime, TaskID: seqNum},
		TimerID:         ti.TimerID,
		TaskCreated:     ti.TaskID == TimerTaskStatusCreated}
	tb.insertTimer(timer)
	tb.logger.Debugf("Added User Timeout for timer ID: %s", ti.TimerID)
}

// GetUserTimerTaskIfNeeded - if we need create a timer task for the user timers
func (tb *timerBuilder) GetUserTimerTaskIfNeeded(msBuilder *mutableStateBuilder) persistence.Task {
	if !tb.isLoadedUserTimers {
		return nil
	}
	timerTask := tb.firstTimerTask()
	if timerTask != nil {
		// Update the task ID tracking if it has created timer task or not.
		ti := tb.pendingUserTimers[tb.userTimers[0].TimerID]
		ti.TaskID = 1
		// TODO: We append updates to timer tasks twice.  Why?
		msBuilder.UpdateUserTimer(ti.TimerID, ti)
	}
	return timerTask
}

// GetUserTimers - Get all user timers.
func (tb *timerBuilder) GetUserTimers(msBuilder *mutableStateBuilder) timers {
	tb.loadUserTimers(msBuilder)
	return tb.userTimers
}

// GetUserTimer - Get a specific user timer.
func (tb *timerBuilder) GetUserTimer(timerID string) (bool, *persistence.TimerInfo) {
	ti, ok := tb.pendingUserTimers[timerID]
	return ok, ti
}

// IsTimerExpired - Whether a timer is expired w.r.t reference time.
func (tb *timerBuilder) IsTimerExpired(td *timerDetails, referenceTime time.Time) bool {
	// Cql timestamp is in milli sec resolution, here we do the check in terms of second resolution.
	expiry := td.TimerSequenceID.VisibilityTimestamp.Unix()
	return expiry <= referenceTime.Unix()
}

func (tb *timerBuilder) GetActivityTimers(msBuilder *mutableStateBuilder) timers {
	tb.loadActivityTimers(msBuilder)
	return tb.activityTimers
}

// GetActivityTimerTaskIfNeeded - if we need create a activity timer task for the activities
func (tb *timerBuilder) GetActivityTimerTaskIfNeeded(msBuilder *mutableStateBuilder) persistence.Task {
	if !tb.isLoadedActivityTimers {
		tb.loadActivityTimers(msBuilder)
	}

	timerTask := tb.firstActivityTimerTask()
	if timerTask != nil {
		// Update the task ID tracking if it has created timer task or not.
		td := tb.activityTimers[0]
		ai := tb.pendingActivityTimers[td.ActivityID]
		at := timerTask.(*persistence.ActivityTimeoutTask)
		ai.TimerTaskStatus = ai.TimerTaskStatus | getActivityTimerStatus(w.TimeoutType(at.TimeoutType))
		msBuilder.UpdateActivity(ai)

		tb.logger.Debugf("%s: Adding Activity Timeout: with timeout: %v sec, ExpiryTime: %s, TimeoutType: %v, EventID: %v",
			time.Now(), td.TimeoutSec, at.VisibilityTimestamp, td.TimeoutType.String(), at.EventID)
	}
	return timerTask
}

// loadUserTimers - Load all user timers from mutable state.
func (tb *timerBuilder) loadUserTimers(msBuilder *mutableStateBuilder) {
	tb.pendingUserTimers = msBuilder.pendingTimerInfoIDs
	tb.userTimers = make(timers, 0, len(msBuilder.pendingTimerInfoIDs))
	for _, v := range msBuilder.pendingTimerInfoIDs {
		seqNum := tb.localSeqNumGen.NextSeq()
		td := &timerDetails{
			TimerSequenceID: TimerSequenceID{VisibilityTimestamp: v.ExpiryTime, TaskID: seqNum},
			TimerID:         v.TimerID,
			TaskCreated:     v.TaskID == TimerTaskStatusCreated}
		tb.userTimers = append(tb.userTimers, td)
	}
	sort.Sort(tb.userTimers)
	tb.isLoadedUserTimers = true
}

func (tb *timerBuilder) loadActivityTimers(msBuilder *mutableStateBuilder) {
	tb.pendingActivityTimers = msBuilder.pendingActivityInfoIDs
	tb.activityTimers = make(timers, 0, len(msBuilder.pendingActivityInfoIDs))
	for _, v := range msBuilder.pendingActivityInfoIDs {
		if v.ScheduleID != emptyEventID {
			scheduleToCloseExpiry := v.ScheduledTime.Add(time.Duration(v.ScheduleToCloseTimeout) * time.Second)
			td := &timerDetails{
				TimerSequenceID: TimerSequenceID{VisibilityTimestamp: scheduleToCloseExpiry},
				ActivityID:      v.ScheduleID,
				EventID:         v.ScheduleID,
				TimeoutSec:      v.ScheduleToCloseTimeout,
				TimeoutType:     w.TimeoutTypeScheduleToClose,
				TaskCreated:     (v.TimerTaskStatus & TimerTaskStatusCreatedScheduleToClose) != 0}
			tb.activityTimers = append(tb.activityTimers, td)

			if v.StartedID != emptyEventID {
				startToCloseExpiry := v.StartedTime.Add(time.Duration(v.StartToCloseTimeout) * time.Second)
				td := &timerDetails{
					TimerSequenceID: TimerSequenceID{VisibilityTimestamp: startToCloseExpiry},
					ActivityID:      v.ScheduleID,
					EventID:         v.StartedID,
					TimeoutType:     w.TimeoutTypeStartToClose,
					TimeoutSec:      v.StartToCloseTimeout,
					TaskCreated:     (v.TimerTaskStatus & TimerTaskStatusCreatedStartToClose) != 0}
				tb.activityTimers = append(tb.activityTimers, td)
				if v.HeartbeatTimeout > 0 {
					lastHeartBeatTS := v.LastHeartBeatUpdatedTime
					if lastHeartBeatTS.IsZero() {
						lastHeartBeatTS = v.StartedTime
					}
					heartBeatExpiry := lastHeartBeatTS.Add(time.Duration(v.HeartbeatTimeout) * time.Second)
					td := &timerDetails{
						TimerSequenceID: TimerSequenceID{VisibilityTimestamp: heartBeatExpiry},
						ActivityID:      v.ScheduleID,
						EventID:         v.StartedID,
						TimeoutType:     w.TimeoutTypeHeartbeat,
						TimeoutSec:      v.HeartbeatTimeout,
						TaskCreated:     (v.TimerTaskStatus & TimerTaskStatusCreatedHeartbeat) != 0}
					tb.activityTimers = append(tb.activityTimers, td)
				}
			} else {
				scheduleToStartExpiry := v.ScheduledTime.Add(time.Duration(v.ScheduleToStartTimeout) * time.Second)
				td := &timerDetails{
					TimerSequenceID: TimerSequenceID{VisibilityTimestamp: scheduleToStartExpiry},
					ActivityID:      v.ScheduleID,
					EventID:         v.ScheduleID,
					TimeoutSec:      v.ScheduleToStartTimeout,
					TimeoutType:     w.TimeoutTypeScheduleToStart,
					TaskCreated:     (v.TimerTaskStatus & TimerTaskStatusCreatedScheduleToStart) != 0}
				tb.activityTimers = append(tb.activityTimers, td)
			}
		}
	}
	sort.Sort(tb.activityTimers)
	tb.isLoadedActivityTimers = true
}

func (tb *timerBuilder) createDeleteHistoryEventTimerTask(d time.Duration) *persistence.DeleteHistoryEventTask {
	expiryTime := tb.timeSource.Now().Add(d)
	return &persistence.DeleteHistoryEventTask{
		VisibilityTimestamp: expiryTime,
	}
}

// createDecisionTimeoutTask - Creates a decision timeout task.
func (tb *timerBuilder) createDecisionTimeoutTask(fireTimeOut int32, eventID, attempt int64,
	timeoutType w.TimeoutType) *persistence.DecisionTimeoutTask {
	expiryTime := tb.timeSource.Now().Add(time.Duration(fireTimeOut) * time.Second)
	return &persistence.DecisionTimeoutTask{
		VisibilityTimestamp: expiryTime,
		TimeoutType:         int(timeoutType),
		EventID:             eventID,
		ScheduleAttempt:     attempt,
	}
}

// createActivityTimeoutTask - Creates a activity timeout task.
func (tb *timerBuilder) createActivityTimeoutTask(fireTimeOut int32, timeoutType w.TimeoutType,
	eventID int64, baseTime *time.Time) *persistence.ActivityTimeoutTask {
	var expiryTime time.Time
	if baseTime != nil {
		expiryTime = baseTime.Add(time.Duration(fireTimeOut) * time.Second)
	} else {
		expiryTime = tb.timeSource.Now().Add(time.Duration(fireTimeOut) * time.Second)
	}

	return &persistence.ActivityTimeoutTask{
		VisibilityTimestamp: expiryTime,
		TimeoutType:         int(timeoutType),
		EventID:             eventID,
	}
}

func (tb *timerBuilder) loadUserTimer(expires time.Time, timerID string, taskCreated bool) (*timerDetails, bool) {
	seqNum := tb.localSeqNumGen.NextSeq()
	timer := &timerDetails{
		TimerSequenceID: TimerSequenceID{VisibilityTimestamp: expires, TaskID: seqNum},
		TimerID:         timerID,
		TaskCreated:     taskCreated}
	isFirst := tb.insertTimer(timer)
	return timer, isFirst
}

func (tb *timerBuilder) insertTimer(td *timerDetails) bool {
	size := len(tb.userTimers)
	i := sort.Search(size,
		func(i int) bool { return !compareTimerIDLess(&tb.userTimers[i].TimerSequenceID, &td.TimerSequenceID) })
	if i == size {
		tb.userTimers = append(tb.userTimers, td)
	} else {
		tb.userTimers = append(tb.userTimers[:i], append(timers{td}, tb.userTimers[i:]...)...)
	}
	return i == 0 // This is the first timer in the list.
}

func (tb *timerBuilder) firstTimerTask() persistence.Task {
	if len(tb.userTimers) > 0 && !tb.userTimers[0].TaskCreated {
		return tb.createNewTask(tb.userTimers[0])
	}
	return nil
}

func (tb *timerBuilder) firstActivityTimerTask() persistence.Task {
	if len(tb.activityTimers) > 0 && !tb.activityTimers[0].TaskCreated {
		return tb.createNewTask(tb.activityTimers[0])
	}
	return nil
}

func (tb *timerBuilder) createNewTask(td *timerDetails) persistence.Task {
	// Create a copy of this task.
	if td.TimerID != "" {
		tt := tb.pendingUserTimers[td.TimerID]
		return &persistence.UserTimerTask{
			VisibilityTimestamp: td.TimerSequenceID.VisibilityTimestamp,
			EventID:             tt.StartedID,
		}
	} else if td.ActivityID != 0 && td.ActivityID != emptyEventID {
		return &persistence.ActivityTimeoutTask{
			VisibilityTimestamp: td.TimerSequenceID.VisibilityTimestamp,
			EventID:             td.EventID,
			TimeoutType:         int(td.TimeoutType),
		}
	}
	return nil
}

func compareTimerIDLess(first *TimerSequenceID, second *TimerSequenceID) bool {
	if first.VisibilityTimestamp.Before(second.VisibilityTimestamp) {
		return true
	}
	if first.VisibilityTimestamp.Equal(second.VisibilityTimestamp) {
		return first.TaskID < second.TaskID
	}
	return false
}

func getActivityTimerStatus(timeoutType w.TimeoutType) int32 {
	switch timeoutType {
	case w.TimeoutTypeHeartbeat:
		return TimerTaskStatusCreatedHeartbeat
	case w.TimeoutTypeScheduleToStart:
		return TimerTaskStatusCreatedScheduleToStart
	case w.TimeoutTypeScheduleToClose:
		return TimerTaskStatusCreatedScheduleToClose
	case w.TimeoutTypeStartToClose:
		return TimerTaskStatusCreatedStartToClose
	}
	panic("invalid timeout type")
}
