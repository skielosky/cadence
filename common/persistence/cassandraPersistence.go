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

package persistence

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

// Guidelines for creating new special UUID constants
// Each UUID should be of the form: E0000000-R000-f000-f000-00000000000x
// Where x is any hexadecimal value, E represents the entity type valid values are:
// E = {DomainID = 1, WorkflowID = 2, RunID = 3}
// R represents row type in executions table, valid values are:
// R = {Shard = 1, Execution = 2, Transfer = 3, Timer = 4}
const (
	cassandraProtoVersion = 4
	defaultSessionTimeout = 10 * time.Second
	// Special Domains related constants
	emptyDomainID = "10000000-0000-f000-f000-000000000000"
	// Special Run IDs
	emptyRunID                          = "30000000-0000-f000-f000-000000000000"
	permanentRunID                      = "30000000-0000-f000-f000-000000000001"
	transferTaskTypeTransferTargetRunID = "30000000-0000-f000-f000-000000000002"
	// Special Workflow IDs
	transferTaskTransferTargetWorkflowID = "20000000-0000-f000-f000-000000000001"
	// Row Constants for Shard Row
	rowTypeShardDomainID   = "10000000-1000-f000-f000-000000000000"
	rowTypeShardWorkflowID = "20000000-1000-f000-f000-000000000000"
	rowTypeShardRunID      = "30000000-1000-f000-f000-000000000000"
	// Row Constants for Transfer Task Row
	rowTypeTransferDomainID   = "10000000-3000-f000-f000-000000000000"
	rowTypeTransferWorkflowID = "20000000-3000-f000-f000-000000000000"
	rowTypeTransferRunID      = "30000000-3000-f000-f000-000000000000"
	// Row Constants for Timer Task Row
	rowTypeTimerDomainID   = "10000000-4000-f000-f000-000000000000"
	rowTypeTimerWorkflowID = "20000000-4000-f000-f000-000000000000"
	rowTypeTimerRunID      = "30000000-4000-f000-f000-000000000000"
	// Special TaskId constants
	rowTypeExecutionTaskID  = int64(-10)
	rowTypeShardTaskID      = int64(-11)
	emptyInitiatedID        = int64(-7)
	defaultDeleteTTLSeconds = int64(time.Hour*24*7) / int64(time.Second) // keep deleted records for 7 days

	// minimum current execution retention TTL when current execution is deleted, in seconds
	minCurrentExecutionRetentionTTL = int32(24 * time.Hour / time.Second)

	stickyTaskListTTL = int32(24 * time.Hour / time.Second) // if sticky task_list stopped being updated, remove it in one day
)

const (
	// Row types for table executions
	rowTypeShard = iota
	rowTypeExecution
	rowTypeTransferTask
	rowTypeTimerTask
)

const (
	// Row types for table tasks
	rowTypeTask = iota
	rowTypeTaskList
)

const (
	taskListTaskID = -12345
	initialRangeID = 1 // Id of the first range of a new task list
)

const (
	templateShardType = `{` +
		`shard_id: ?, ` +
		`owner: ?, ` +
		`range_id: ?, ` +
		`stolen_since_renew: ?, ` +
		`updated_at: ?, ` +
		`transfer_ack_level: ?, ` +
		`timer_ack_level: ?, ` +
		`cluster_transfer_ack_level: ?, ` +
		`cluster_timer_ack_level: ?` +
		`}`

	templateWorkflowExecutionType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`parent_domain_id: ?, ` +
		`parent_workflow_id: ?, ` +
		`parent_run_id: ?, ` +
		`initiated_id: ?, ` +
		`completion_event: ?, ` +
		`task_list: ?, ` +
		`workflow_type_name: ?, ` +
		`workflow_timeout: ?, ` +
		`decision_task_timeout: ?, ` +
		`execution_context: ?, ` +
		`state: ?, ` +
		`close_status: ?, ` +
		`last_first_event_id: ?, ` +
		`next_event_id: ?, ` +
		`last_processed_event: ?, ` +
		`start_time: ?, ` +
		`last_updated_time: ?, ` +
		`create_request_id: ?, ` +
		`decision_schedule_id: ?, ` +
		`decision_started_id: ?, ` +
		`decision_request_id: ?, ` +
		`decision_timeout: ?, ` +
		`decision_attempt: ?, ` +
		`decision_timestamp: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?, ` +
		`sticky_task_list: ?, ` +
		`sticky_schedule_to_start_timeout: ?,` +
		`client_library_version: ?, ` +
		`client_feature_version: ?, ` +
		`client_impl: ?` +
		`}`

	templateTransferTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`task_id: ?, ` +
		`target_domain_id: ?, ` +
		`target_workflow_id: ?, ` +
		`target_run_id: ?, ` +
		`target_child_workflow_only: ?, ` +
		`task_list: ?, ` +
		`type: ?, ` +
		`schedule_id: ?` +
		`}`

	templateTimerTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`visibility_ts: ?, ` +
		`task_id: ?, ` +
		`type: ?, ` +
		`timeout_type: ?, ` +
		`event_id: ?, ` +
		`schedule_attempt: ?` +
		`}`

	templateActivityInfoType = `{` +
		`schedule_id: ?, ` +
		`scheduled_event: ?, ` +
		`scheduled_time: ?, ` +
		`started_id: ?, ` +
		`started_event: ?, ` +
		`started_time: ?, ` +
		`activity_id: ?, ` +
		`request_id: ?, ` +
		`details: ?, ` +
		`schedule_to_start_timeout: ?, ` +
		`schedule_to_close_timeout: ?, ` +
		`start_to_close_timeout: ?, ` +
		`heart_beat_timeout: ?, ` +
		`cancel_requested: ?, ` +
		`cancel_request_id: ?, ` +
		`last_hb_updated_time: ?, ` +
		`timer_task_status: ?` +
		`}`

	templateTimerInfoType = `{` +
		`timer_id: ?, ` +
		`started_id: ?, ` +
		`expiry_time: ?, ` +
		`task_id: ?` +
		`}`

	templateChildExecutionInfoType = `{` +
		`initiated_id: ?, ` +
		`initiated_event: ?, ` +
		`started_id: ?, ` +
		`started_event: ?, ` +
		`create_request_id: ?` +
		`}`

	templateRequestCancelInfoType = `{` +
		`initiated_id: ?, ` +
		`cancel_request_id: ? ` +
		`}`

	templateSignalInfoType = `{` +
		`initiated_id: ?, ` +
		`signal_request_id: ?, ` +
		`signal_name: ?, ` +
		`input: ?, ` +
		`control: ?` +
		`}`

	templateTaskListType = `{` +
		`domain_id: ?, ` +
		`name: ?, ` +
		`type: ?, ` +
		`ack_level: ?, ` +
		`kind: ? ` +
		`}`

	templateTaskType = `{` +
		`domain_id: ?, ` +
		`workflow_id: ?, ` +
		`run_id: ?, ` +
		`schedule_id: ?` +
		`}`

	templateCreateShardQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, shard, range_id)` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ` + templateShardType + `, ?) IF NOT EXISTS`

	templateGetShardQuery = `SELECT shard ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateShardQuery = `UPDATE executions ` +
		`SET shard = ` + templateShardType + `, range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateCurrentWorkflowExecutionQuery = `UPDATE executions USING TTL 0 ` +
		`SET current_run_id = ?, execution = {run_id: ?, create_request_id: ?, state: ?, close_status: ?}` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF current_run_id = ? `

	templateCreateWorkflowExecutionQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}) IF NOT EXISTS USING TTL 0 `

	templateCreateWorkflowExecutionQuery2 = `INSERT INTO executions (` +
		`shard_id, domain_id, workflow_id, run_id, type, execution, next_event_id, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateWorkflowExecutionType + `, ?, ?, ?) `

	templateCreateTransferTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, transfer, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTransferTaskType + `, ?, ?)`

	templateCreateTimerTaskQuery = `INSERT INTO executions (` +
		`shard_id, type, domain_id, workflow_id, run_id, timer, visibility_ts, task_id) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTimerTaskType + `, ?, ?)`

	templateUpdateLeaseQuery = `UPDATE executions ` +
		`SET range_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateGetWorkflowExecutionQuery = `SELECT execution, activity_map, timer_map, child_executions_map, request_cancel_map, signal_map, signal_requested, buffered_events_list ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetCurrentExecutionQuery = `SELECT current_run_id, execution ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateUpdateWorkflowExecutionQuery = `UPDATE executions ` +
		`SET execution = ` + templateWorkflowExecutionType + `, next_event_id = ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateActivityInfoQuery = `UPDATE executions ` +
		`SET activity_map[ ? ] =` + templateActivityInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateTimerInfoQuery = `UPDATE executions ` +
		`SET timer_map[ ? ] =` + templateTimerInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateChildExecutionInfoQuery = `UPDATE executions ` +
		`SET child_executions_map[ ? ] =` + templateChildExecutionInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateRequestCancelInfoQuery = `UPDATE executions ` +
		`SET request_cancel_map[ ? ] =` + templateRequestCancelInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateSignalInfoQuery = `UPDATE executions ` +
		`SET signal_map[ ? ] =` + templateSignalInfoType + ` ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateUpdateSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateAppendBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = buffered_events_list + ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteBufferedEventsQuery = `UPDATE executions ` +
		`SET buffered_events_list = [] ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteActivityInfoQuery = `DELETE activity_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteTimerInfoQuery = `DELETE timer_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteChildExecutionInfoQuery = `DELETE child_executions_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteRequestCancelInfoQuery = `DELETE request_cancel_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteWorkflowExecutionQueryWithTTL = `INSERT INTO executions ` +
		`(shard_id, type, domain_id, workflow_id, run_id, visibility_ts, task_id, current_run_id, execution) ` +
		`VALUES(?, ?, ?, ?, ?, ?, ?, ?, {run_id: ?, create_request_id: ?, state: ?, close_status: ?}) USING TTL ? `

	templateDeleteSignalInfoQuery = `DELETE signal_map[ ? ] ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateDeleteWorkflowExecutionMutableStateQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? `

	templateDeleteWorkflowExecutionSignalRequestedQuery = `UPDATE executions ` +
		`SET signal_requested = signal_requested - ? ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ? ` +
		`IF next_event_id = ?`

	templateGetTransferTasksQuery = `SELECT transfer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id > ? ` +
		`and task_id <= ? LIMIT ?`

	templateCompleteTransferTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ? ` +
		`and run_id = ? ` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateGetTimerTasksQuery = `SELECT timer ` +
		`FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ?` +
		`and domain_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts >= ? ` +
		`and visibility_ts < ? LIMIT ?`

	templateCompleteTimerTaskQuery = `DELETE FROM executions ` +
		`WHERE shard_id = ? ` +
		`and type = ? ` +
		`and domain_id = ? ` +
		`and workflow_id = ?` +
		`and run_id = ?` +
		`and visibility_ts = ? ` +
		`and task_id = ?`

	templateCreateTaskQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `)`

	templateCreateTaskWithTTLQuery = `INSERT INTO tasks (` +
		`domain_id, task_list_name, task_list_type, type, task_id, task) ` +
		`VALUES(?, ?, ?, ?, ?, ` + templateTaskType + `) USING TTL ?`

	templateGetTasksQuery = `SELECT task_id, task ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id > ? ` +
		`and task_id <= ? LIMIT ?`

	templateCompleteTaskQuery = `DELETE FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateGetTaskList = `SELECT ` +
		`range_id, ` +
		`task_list ` +
		`FROM tasks ` +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ?`

	templateInsertTaskListQuery = `INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ` + templateTaskListType + `) IF NOT EXISTS`

	templateUpdateTaskListQuery = `UPDATE tasks SET ` +
		`range_id = ?, ` +
		`task_list = ` + templateTaskListType + " " +
		`WHERE domain_id = ? ` +
		`and task_list_name = ? ` +
		`and task_list_type = ? ` +
		`and type = ? ` +
		`and task_id = ? ` +
		`IF range_id = ?`

	templateUpdateTaskListQueryWithTTL = `INSERT INTO tasks (` +
		`domain_id, ` +
		`task_list_name, ` +
		`task_list_type, ` +
		`type, ` +
		`task_id, ` +
		`range_id, ` +
		`task_list ` +
		`) VALUES (?, ?, ?, ?, ?, ?, ` + templateTaskListType + `) USING TTL ?`
)

var (
	defaultDateTime            = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	defaultVisibilityTimestamp = common.UnixNanoToCQLTimestamp(defaultDateTime.UnixNano())
)

type (
	cassandraPersistence struct {
		session            *gocql.Session
		lowConslevel       gocql.Consistency
		shardID            int
		currentClusterName string
		logger             bark.Logger
	}
)

// NewCassandraShardPersistence is used to create an instance of ShardManager implementation
func NewCassandraShardPersistence(hosts string, port int, user, password, dc string, keyspace string,
	currentClusterName string, logger bark.Logger) (ShardManager, error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraPersistence{shardID: -1, session: session, lowConslevel: gocql.One, currentClusterName: currentClusterName, logger: logger}, nil
}

// NewCassandraWorkflowExecutionPersistence is used to create an instance of workflowExecutionManager implementation
func NewCassandraWorkflowExecutionPersistence(shardID int, session *gocql.Session,
	logger bark.Logger) (ExecutionManager, error) {
	return &cassandraPersistence{shardID: shardID, session: session, lowConslevel: gocql.One, logger: logger}, nil
}

// NewCassandraTaskPersistence is used to create an instance of TaskManager implementation
func NewCassandraTaskPersistence(hosts string, port int, user, password, dc string, keyspace string,
	logger bark.Logger) (TaskManager, error) {
	cluster := common.NewCassandraCluster(hosts, port, user, password, dc)
	cluster.Keyspace = keyspace
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cassandraPersistence{shardID: -1, session: session, lowConslevel: gocql.One, logger: logger}, nil
}

// Close releases the underlying resources held by this object
func (d *cassandraPersistence) Close() {
	if d.session != nil {
		d.session.Close()
	}
}

func (d *cassandraPersistence) CreateShard(request *CreateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo
	query := d.session.Query(templateCreateShardQuery,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.TransferAckLevel,
		shardInfo.TimerAckLevel,
		shardInfo.ClusterTransferAckLevel,
		shardInfo.ClusterTimerAckLevel,
		shardInfo.RangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateShard operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateShard operation failed. Error : %v", err),
		}
	}

	if !applied {
		shard := previous["shard"].(map[string]interface{})
		return &ShardAlreadyExistError{
			Msg: fmt.Sprintf("Shard already exists in executions table.  ShardId: %v, RangeId: %v",
				shard["shard_id"], shard["range_id"]),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetShard(request *GetShardRequest) (*GetShardResponse, error) {
	shardID := request.ShardID
	query := d.session.Query(templateGetShardQuery,
		shardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID).Consistency(d.lowConslevel)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Shard not found.  ShardId: %v", shardID),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetShard operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetShard operation failed. Error: %v", err),
		}
	}

	info := createShardInfo(d.currentClusterName, result["shard"].(map[string]interface{}))

	return &GetShardResponse{ShardInfo: info}, nil
}

func (d *cassandraPersistence) UpdateShard(request *UpdateShardRequest) error {
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	shardInfo := request.ShardInfo

	query := d.session.Query(templateUpdateShardQuery,
		shardInfo.ShardID,
		shardInfo.Owner,
		shardInfo.RangeID,
		shardInfo.StolenSinceRenew,
		cqlNowTimestamp,
		shardInfo.TransferAckLevel,
		shardInfo.TimerAckLevel,
		shardInfo.ClusterTransferAckLevel,
		shardInfo.ClusterTimerAckLevel,
		shardInfo.RangeID,
		shardInfo.ShardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.PreviousRangeID)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateShard operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ShardOwnershipLostError{
			ShardID: d.shardID,
			Msg: fmt.Sprintf("Failed to update shard.  previous_range_id: %v, columns: (%v)",
				request.PreviousRangeID, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (
	*CreateWorkflowExecutionResponse, error) {
	transferTaskID := uuid.New()
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())
	batch := d.session.NewBatch(gocql.LoggedBatch)

	d.CreateWorkflowExecutionWithinBatch(request, batch, cqlNowTimestamp)

	d.createTransferTasks(batch, request.TransferTasks, request.DomainID, *request.Execution.WorkflowId,
		*request.Execution.RunId, cqlNowTimestamp)
	d.createTimerTasks(batch, request.TimerTasks, nil, request.DomainID, *request.Execution.WorkflowId,
		*request.Execution.RunId, cqlNowTimestamp)

	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return nil, &TimeoutError{Msg: fmt.Sprintf("CreateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
		}

	}

	if !applied {
		// There can be two reasons why the query does not get applied. Either the RangeID has changed, or
		// the workflow is already started. Check the row info returned by Cassandra to figure out which one it is.
	GetFailureReasonLoop:
		for {
			rowType, ok := previous["type"].(int)
			if !ok {
				// This should never happen, as all our rows have the type field.
				break GetFailureReasonLoop
			}

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// CreateWorkflowExecution failed because rangeID was modified
					return nil, &ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}

			} else {
				var columns []string
				for k, v := range previous {
					columns = append(columns, fmt.Sprintf("%s=%v", k, v))
				}

				if execution, ok := previous["execution"].(map[string]interface{}); ok {
					// CreateWorkflowExecution failed because it already exists
					executionInfo := createWorkflowExecutionInfo(execution)
					msg := fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeID: %v, columns: (%v)",
						request.Execution.GetWorkflowId(), executionInfo.RunID, request.RangeID, strings.Join(columns, ","))
					return nil, &WorkflowExecutionAlreadyStartedError{
						Msg:            msg,
						StartRequestID: executionInfo.CreateRequestID,
						RunID:          executionInfo.RunID,
						State:          executionInfo.State,
						CloseStatus:    executionInfo.CloseStatus,
					}
				}
			}

			previous = make(map[string]interface{})
			if !iter.MapScan(previous) {
				// Cassandra returns the actual row that caused a condition failure, so we should always return
				// from the checks above, but just in case.
				break GetFailureReasonLoop
			}
		}

		// At this point we only know that the write was not applied.
		// Return the row information returned by Cassandra.
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create workflow execution.  Request RangeID: %v, columns: (%v)",
				request.RangeID, strings.Join(columns, ",")),
		}
	}

	return &CreateWorkflowExecutionResponse{TaskID: transferTaskID}, nil
}

func (d *cassandraPersistence) CreateWorkflowExecutionWithinBatch(request *CreateWorkflowExecutionRequest,
	batch *gocql.Batch, cqlNowTimestamp int64) {

	parentDomainID := emptyDomainID
	parentWorkflowID := ""
	parentRunID := emptyRunID
	initiatedID := emptyInitiatedID
	state := WorkflowStateRunning
	closeStatus := WorkflowCloseStatusNone
	if request.ParentExecution != nil {
		parentDomainID = request.ParentDomainID
		parentWorkflowID = *request.ParentExecution.WorkflowId
		parentRunID = *request.ParentExecution.RunId
		initiatedID = request.InitiatedID
		state = WorkflowStateCreated
	}

	if request.ContinueAsNew {
		batch.Query(templateUpdateCurrentWorkflowExecutionQuery,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			request.PreviousRunID,
		)
	} else {
		batch.Query(templateCreateWorkflowExecutionQuery,
			d.shardID,
			rowTypeExecution,
			request.DomainID,
			*request.Execution.WorkflowId,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			*request.Execution.RunId,
			*request.Execution.RunId,
			request.RequestID,
			state,
			closeStatus,
		)
	}

	batch.Query(templateCreateWorkflowExecutionQuery2,
		d.shardID,
		request.DomainID,
		*request.Execution.WorkflowId,
		*request.Execution.RunId,
		rowTypeExecution,
		request.DomainID,
		*request.Execution.WorkflowId,
		*request.Execution.RunId,
		parentDomainID,
		parentWorkflowID,
		parentRunID,
		initiatedID,
		nil,
		request.TaskList,
		request.WorkflowTypeName,
		request.WorkflowTimeout,
		request.DecisionTimeoutValue,
		request.ExecutionContext,
		WorkflowStateCreated,
		WorkflowCloseStatusNone,
		common.FirstEventID,
		request.NextEventID,
		request.LastProcessedEvent,
		cqlNowTimestamp,
		cqlNowTimestamp,
		request.RequestID,
		request.DecisionScheduleID,
		request.DecisionStartedID,
		"", // Decision Start Request ID
		request.DecisionStartToCloseTimeout,
		0,
		0,
		false,
		"",
		"", // sticky_task_list (no sticky tasklist for new workflow execution)
		0,  // sticky_schedule_to_start_timeout
		"", // client_library_version
		"", // client_feature_version
		"", // client_impl
		request.NextEventID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)
}

func (d *cassandraPersistence) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (
	*GetWorkflowExecutionResponse, error) {
	execution := request.Execution
	query := d.session.Query(templateGetWorkflowExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		*execution.WorkflowId,
		*execution.RunId,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					*execution.WorkflowId, *execution.RunId),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Error: %v", err),
		}
	}

	state := &WorkflowMutableState{}
	info := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))
	state.ExecutionInfo = info

	activityInfos := make(map[int64]*ActivityInfo)
	aMap := result["activity_map"].(map[int64]map[string]interface{})
	for key, value := range aMap {
		info := createActivityInfo(value)
		activityInfos[key] = info
	}
	state.ActivitInfos = activityInfos

	timerInfos := make(map[string]*TimerInfo)
	tMap := result["timer_map"].(map[string]map[string]interface{})
	for key, value := range tMap {
		info := createTimerInfo(value)
		timerInfos[key] = info
	}
	state.TimerInfos = timerInfos

	childExecutionInfos := make(map[int64]*ChildExecutionInfo)
	cMap := result["child_executions_map"].(map[int64]map[string]interface{})
	for key, value := range cMap {
		info := createChildExecutionInfo(value)
		childExecutionInfos[key] = info
	}
	state.ChildExecutionInfos = childExecutionInfos

	requestCancelInfos := make(map[int64]*RequestCancelInfo)
	rMap := result["request_cancel_map"].(map[int64]map[string]interface{})
	for key, value := range rMap {
		info := createRequestCancelInfo(value)
		requestCancelInfos[key] = info
	}
	state.RequestCancelInfos = requestCancelInfos

	signalInfos := make(map[int64]*SignalInfo)
	sMap := result["signal_map"].(map[int64]map[string]interface{})
	for key, value := range sMap {
		info := createSignalInfo(value)
		signalInfos[key] = info
	}
	state.SignalInfos = signalInfos

	signalRequestedIDs := make(map[string]struct{})
	sList := result["signal_requested"].([]gocql.UUID)
	for _, v := range sList {
		signalRequestedIDs[v.String()] = struct{}{}
	}
	state.SignalRequestedIDs = signalRequestedIDs

	eList := result["buffered_events_list"].([]map[string]interface{})
	bufferedEvents := make([]*SerializedHistoryEventBatch, 0, len(eList))
	for _, v := range eList {
		eventBatch := createSerializedHistoryEventBatch(v)
		bufferedEvents = append(bufferedEvents, eventBatch)
	}
	state.BufferedEvents = bufferedEvents

	return &GetWorkflowExecutionResponse{State: state}, nil
}

func (d *cassandraPersistence) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	executionInfo := request.ExecutionInfo
	cqlNowTimestamp := common.UnixNanoToCQLTimestamp(time.Now().UnixNano())

	batch := d.session.NewBatch(gocql.LoggedBatch)
	batch.Query(templateUpdateWorkflowExecutionQuery,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		executionInfo.ParentDomainID,
		executionInfo.ParentWorkflowID,
		executionInfo.ParentRunID,
		executionInfo.InitiatedID,
		executionInfo.CompletionEvent,
		executionInfo.TaskList,
		executionInfo.WorkflowTypeName,
		executionInfo.WorkflowTimeout,
		executionInfo.DecisionTimeoutValue,
		executionInfo.ExecutionContext,
		executionInfo.State,
		executionInfo.CloseStatus,
		executionInfo.LastFirstEventID,
		executionInfo.NextEventID,
		executionInfo.LastProcessedEvent,
		executionInfo.StartTimestamp,
		cqlNowTimestamp,
		executionInfo.CreateRequestID,
		executionInfo.DecisionScheduleID,
		executionInfo.DecisionStartedID,
		executionInfo.DecisionRequestID,
		executionInfo.DecisionTimeout,
		executionInfo.DecisionAttempt,
		executionInfo.DecisionTimestamp,
		executionInfo.CancelRequested,
		executionInfo.CancelRequestID,
		executionInfo.StickyTaskList,
		executionInfo.StickyScheduleToStartTimeout,
		executionInfo.ClientLibraryVersion,
		executionInfo.ClientFeatureVersion,
		executionInfo.ClientImpl,
		executionInfo.NextEventID,
		d.shardID,
		rowTypeExecution,
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID,
		request.Condition)

	d.createTransferTasks(batch, request.TransferTasks, executionInfo.DomainID, executionInfo.WorkflowID,
		executionInfo.RunID, cqlNowTimestamp)

	d.createTimerTasks(batch, request.TimerTasks, request.DeleteTimerTask, request.ExecutionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, cqlNowTimestamp)

	d.updateActivityInfos(batch, request.UpsertActivityInfos, request.DeleteActivityInfo, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateTimerInfos(batch, request.UpserTimerInfos, request.DeleteTimerInfos, executionInfo.DomainID,
		executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateChildExecutionInfos(batch, request.UpsertChildExecutionInfos, request.DeleteChildExecutionInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateRequestCancelInfos(batch, request.UpsertRequestCancelInfos, request.DeleteRequestCancelInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateSignalInfos(batch, request.UpsertSignalInfos, request.DeleteSignalInfo,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateSignalsRequested(batch, request.UpsertSignalRequestedIDs, request.DeleteSignalRequestedID,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	d.updateBufferedEvents(batch, request.NewBufferedEvents, request.ClearBufferedEvents,
		executionInfo.DomainID, executionInfo.WorkflowID, executionInfo.RunID, request.Condition, request.RangeID)

	if request.ContinueAsNew != nil {
		startReq := request.ContinueAsNew
		d.CreateWorkflowExecutionWithinBatch(startReq, batch, cqlNowTimestamp)
		d.createTransferTasks(batch, startReq.TransferTasks, startReq.DomainID, startReq.Execution.GetWorkflowId(),
			startReq.Execution.GetRunId(), cqlNowTimestamp)
		d.createTimerTasks(batch, startReq.TimerTasks, nil, startReq.DomainID, startReq.Execution.GetWorkflowId(),
			startReq.Execution.GetRunId(), cqlNowTimestamp)
	} else if request.FinishExecution {
		retentionInSeconds := request.FinishedExecutionTTL
		if retentionInSeconds <= 0 {
			retentionInSeconds = minCurrentExecutionRetentionTTL
		}
		// Delete WorkflowExecution row representing current execution, by using a TTL
		batch.Query(templateDeleteWorkflowExecutionQueryWithTTL,
			d.shardID,
			rowTypeExecution,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			permanentRunID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			executionInfo.RunID,
			executionInfo.RunID,
			executionInfo.CreateRequestID,
			executionInfo.State,
			executionInfo.CloseStatus,
			retentionInSeconds,
		)
	}

	// Verifies that the RangeID has not changed
	batch.Query(templateUpdateLeaseQuery,
		request.RangeID,
		d.shardID,
		rowTypeShard,
		rowTypeShardDomainID,
		rowTypeShardWorkflowID,
		rowTypeShardRunID,
		defaultVisibilityTimestamp,
		rowTypeShardTaskID,
		request.RangeID,
	)

	previous := make(map[string]interface{})
	applied, iter, err := d.session.MapExecuteBatchCAS(batch, previous)
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()

	if err != nil {
		if isTimeoutError(err) {
			// Write may have succeeded, but we don't know
			// return this info to the caller so they have the option of trying to find out by executing a read
			return &TimeoutError{Msg: fmt.Sprintf("UpdateWorkflowExecution timed out. Error: %v", err)}
		} else if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateWorkflowExecution operation failed. Error: %v", err),
		}
	}

	if !applied {
		// There can be two reasons why the query does not get applied. Either the RangeID has changed, or
		// the next_event_id check failed. Check the row info returned by Cassandra to figure out which one it is.
	GetFailureReasonLoop:
		for {
			rowType, ok := previous["type"].(int)
			if !ok {
				// This should never happen, as all our rows have the type field.
				break GetFailureReasonLoop
			}

			if rowType == rowTypeShard {
				if rangeID, ok := previous["range_id"].(int64); ok && rangeID != request.RangeID {
					// UpdateWorkflowExecution failed because rangeID was modified
					return &ShardOwnershipLostError{
						ShardID: d.shardID,
						Msg: fmt.Sprintf("Failed to update workflow execution.  Request RangeID: %v, Actual RangeID: %v",
							request.RangeID, rangeID),
					}
				}
			} else {
				if nextEventID, ok := previous["next_event_id"].(int64); ok && nextEventID != request.Condition {
					// CreateWorkflowExecution failed because next event ID is unexpected
					return &ConditionFailedError{
						Msg: fmt.Sprintf("Failed to update workflow execution.  Request Condition: %v, Actual Value: %v",
							request.Condition, nextEventID),
					}
				}
			}

			previous = make(map[string]interface{})
			if !iter.MapScan(previous) {
				// Cassandra returns the actual row that caused a condition failure, so we should always return
				// from the checks above, but just in case.
				break GetFailureReasonLoop
			}
		}

		// At this point we only know that the write was not applied.
		// Return the row information returned by Cassandra.
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update workflow execution.  RangeID: %v, Condition: %v, columns: (%v)",
				request.RangeID, request.Condition, strings.Join(columns, ",")),
		}
	}

	return nil
}

func (d *cassandraPersistence) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	query := d.session.Query(templateDeleteWorkflowExecutionMutableStateQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		request.WorkflowID,
		request.RunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("DeleteWorkflowExecution operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse,
	error) {
	query := d.session.Query(templateGetCurrentExecutionQuery,
		d.shardID,
		rowTypeExecution,
		request.DomainID,
		request.WorkflowID,
		permanentRunID,
		defaultVisibilityTimestamp,
		rowTypeExecutionTaskID)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		if err == gocql.ErrNotFound {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v",
					request.WorkflowID),
			}
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
			}
		}

		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetCurrentExecution operation failed. Error: %v", err),
		}
	}

	currentRunID := result["current_run_id"].(gocql.UUID).String()
	executionInfo := createWorkflowExecutionInfo(result["execution"].(map[string]interface{}))
	return &GetCurrentExecutionResponse{
		RunID:          currentRunID,
		StartRequestID: executionInfo.CreateRequestID,
		State:          executionInfo.State,
		CloseStatus:    executionInfo.CloseStatus,
	}, nil
}

func (d *cassandraPersistence) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {

	// Reading transfer tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTransferTasksQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferDomainID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.ReadLevel,
		request.MaxReadLevel,
		request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTransferTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTransferTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createTransferTaskInfo(task["transfer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Tasks = append(response.Tasks, t)
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	query := d.session.Query(templateCompleteTransferTaskQuery,
		d.shardID,
		rowTypeTransferTask,
		rowTypeTransferDomainID,
		rowTypeTransferWorkflowID,
		rowTypeTransferRunID,
		defaultVisibilityTimestamp,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTransferTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	ts := common.UnixNanoToCQLTimestamp(request.VisibilityTimestamp.UnixNano())
	query := d.session.Query(templateCompleteTimerTaskQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerDomainID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		ts,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTimerTask operation failed. Error: %v", err),
		}
	}

	return nil
}

// From TaskManager interface
func (d *cassandraPersistence) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	if len(request.TaskList) == 0 {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList requires non empty task list"),
		}
	}
	query := d.session.Query(templateGetTaskList,
		request.DomainID,
		request.TaskList,
		request.TaskType,
		rowTypeTaskList,
		taskListTaskID,
	)
	var rangeID, ackLevel int64
	var tlDB map[string]interface{}
	err := query.Scan(&rangeID, &tlDB)
	if err != nil {
		if err == gocql.ErrNotFound { // First time task list is used
			query = d.session.Query(templateInsertTaskListQuery,
				request.DomainID,
				request.TaskList,
				request.TaskType,
				rowTypeTaskList,
				taskListTaskID,
				initialRangeID,
				request.DomainID,
				request.TaskList,
				request.TaskType,
				0,
				request.TaskListKind,
			)
		} else if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v",
					request.TaskList, request.TaskType, err),
			}
		} else {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. TaskList: %v, TaskType: %v, Error: %v",
					request.TaskList, request.TaskType, err),
			}
		}
	} else {
		ackLevel = tlDB["ack_level"].(int64)
		taskListKind := tlDB["kind"].(int)
		query = d.session.Query(templateUpdateTaskListQuery,
			rangeID+1,
			request.DomainID,
			&request.TaskList,
			request.TaskType,
			ackLevel,
			taskListKind,
			request.DomainID,
			&request.TaskList,
			request.TaskType,
			rowTypeTaskList,
			taskListTaskID,
			rangeID,
		)
	}
	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("LeaseTaskList operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("LeaseTaskList operation failed. Error : %v", err),
		}
	}
	if !applied {
		previousRangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("LeaseTaskList failed to apply. db rangeID %v", previousRangeID),
		}
	}
	tli := &TaskListInfo{DomainID: request.DomainID, Name: request.TaskList, TaskType: request.TaskType, RangeID: rangeID + 1, AckLevel: ackLevel, Kind: request.TaskListKind}
	return &LeaseTaskListResponse{TaskListInfo: tli}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	tli := request.TaskListInfo

	if tli.Kind == TaskListKindSticky { // if task_list is sticky, then update with TTL
		query := d.session.Query(templateUpdateTaskListQueryWithTTL,
			tli.DomainID,
			&tli.Name,
			tli.TaskType,
			rowTypeTaskList,
			taskListTaskID,
			tli.RangeID,
			tli.DomainID,
			&tli.Name,
			tli.TaskType,
			tli.AckLevel,
			tli.Kind,
			stickyTaskListTTL,
		)
		err := query.Exec()
		if err != nil {
			if isThrottlingError(err) {
				return nil, &workflow.ServiceBusyError{
					Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
				}
			}
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
			}
		}
		return &UpdateTaskListResponse{}, nil
	}

	query := d.session.Query(templateUpdateTaskListQuery,
		tli.RangeID,
		tli.DomainID,
		&tli.Name,
		tli.TaskType,
		tli.AckLevel,
		tli.Kind,
		tli.DomainID,
		&tli.Name,
		tli.TaskType,
		rowTypeTaskList,
		taskListTaskID,
		tli.RangeID,
	)

	previous := make(map[string]interface{})
	applied, err := query.MapScanCAS(previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateTaskList operation failed. Error: %v", err),
		}
	}

	if !applied {
		var columns []string
		for k, v := range previous {
			columns = append(columns, fmt.Sprintf("%s=%v", k, v))
		}

		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to update task list. name: %v, type: %v, rangeID: %v, columns: (%v)",
				tli.Name, tli.TaskType, tli.RangeID, strings.Join(columns, ",")),
		}
	}

	return &UpdateTaskListResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	batch := d.session.NewBatch(gocql.LoggedBatch)
	domainID := request.TaskListInfo.DomainID
	taskList := request.TaskListInfo.Name
	taskListType := request.TaskListInfo.TaskType
	taskListKind := request.TaskListInfo.Kind
	ackLevel := request.TaskListInfo.AckLevel

	for _, task := range request.Tasks {
		scheduleID := task.Data.ScheduleID
		if task.Data.ScheduleToStartTimeout == 0 {
			batch.Query(templateCreateTaskQuery,
				domainID,
				taskList,
				taskListType,
				rowTypeTask,
				task.TaskID,
				domainID,
				task.Execution.GetWorkflowId(),
				task.Execution.GetRunId(),
				scheduleID)
		} else {
			batch.Query(templateCreateTaskWithTTLQuery,
				domainID,
				taskList,
				taskListType,
				rowTypeTask,
				task.TaskID,
				domainID,
				task.Execution.GetWorkflowId(),
				task.Execution.GetRunId(),
				scheduleID,
				task.Data.ScheduleToStartTimeout)
		}
	}

	// The following query is used to ensure that range_id didn't change
	batch.Query(templateUpdateTaskListQuery,
		request.TaskListInfo.RangeID,
		domainID,
		taskList,
		taskListType,
		ackLevel,
		taskListKind,
		domainID,
		taskList,
		taskListType,
		rowTypeTaskList,
		taskListTaskID,
		request.TaskListInfo.RangeID,
	)

	previous := make(map[string]interface{})
	applied, _, err := d.session.MapExecuteBatchCAS(batch, previous)
	if err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CreateTasks operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateTasks operation failed. Error : %v", err),
		}
	}
	if !applied {
		rangeID := previous["range_id"]
		return nil, &ConditionFailedError{
			Msg: fmt.Sprintf("Failed to create task. TaskList: %v, taskListType: %v, rangeID: %v, db rangeID: %v",
				taskList, taskListType, request.TaskListInfo.RangeID, rangeID),
		}
	}

	return &CreateTasksResponse{}, nil
}

// From TaskManager interface
func (d *cassandraPersistence) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if request.ReadLevel > request.MaxReadLevel {
		return &GetTasksResponse{}, nil
	}

	// Reading tasklist tasks need to be quorum level consistent, otherwise we could loose task
	query := d.session.Query(templateGetTasksQuery,
		request.DomainID,
		request.TaskList,
		request.TaskType,
		rowTypeTask,
		request.ReadLevel,
		request.MaxReadLevel,
		request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTasksResponse{}
	task := make(map[string]interface{})
PopulateTasks:
	for iter.MapScan(task) {
		taskID, ok := task["task_id"]
		if !ok { // no tasks, but static column record returned
			continue
		}
		t := createTaskInfo(task["task"].(map[string]interface{}))
		t.TaskID = taskID.(int64)
		response.Tasks = append(response.Tasks, t)
		if len(response.Tasks) == request.BatchSize {
			break PopulateTasks
		}
		task = make(map[string]interface{}) // Reinitialize map as initialized fails on unmarshalling
	}

	if err := iter.Close(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

// From TaskManager interface
func (d *cassandraPersistence) CompleteTask(request *CompleteTaskRequest) error {
	tli := request.TaskList
	query := d.session.Query(templateCompleteTaskQuery,
		tli.DomainID,
		tli.Name,
		tli.TaskType,
		rowTypeTask,
		request.TaskID)

	err := query.Exec()
	if err != nil {
		if isThrottlingError(err) {
			return &workflow.ServiceBusyError{
				Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
			}
		}
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("CompleteTask operation failed. Error: %v", err),
		}
	}

	return nil
}

func (d *cassandraPersistence) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse,
	error) {
	// Reading timer tasks need to be quorum level consistent, otherwise we could loose task
	minTimestamp := common.UnixNanoToCQLTimestamp(request.MinTimestamp.UnixNano())
	maxTimestamp := common.UnixNanoToCQLTimestamp(request.MaxTimestamp.UnixNano())
	query := d.session.Query(templateGetTimerTasksQuery,
		d.shardID,
		rowTypeTimerTask,
		rowTypeTimerDomainID,
		rowTypeTimerWorkflowID,
		rowTypeTimerRunID,
		minTimestamp,
		maxTimestamp,
		request.BatchSize)

	iter := query.Iter()
	if iter == nil {
		return nil, &workflow.InternalServiceError{
			Message: "GetTimerTasks operation failed.  Not able to create query iterator.",
		}
	}

	response := &GetTimerIndexTasksResponse{}
	task := make(map[string]interface{})
	for iter.MapScan(task) {
		t := createTimerTaskInfo(task["timer"].(map[string]interface{}))
		// Reset task map to get it ready for next scan
		task = make(map[string]interface{})

		response.Timers = append(response.Timers, t)
	}
	nextPageToken := iter.PageState()
	response.NextPageToken = make([]byte, len(nextPageToken))
	copy(response.NextPageToken, nextPageToken)

	if err := iter.Close(); err != nil {
		if isThrottlingError(err) {
			return nil, &workflow.ServiceBusyError{
				Message: fmt.Sprintf("GetTimerTasks operation failed. Error: %v", err),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetTimerTasks operation failed. Error: %v", err),
		}
	}

	return response, nil
}

func (d *cassandraPersistence) createTransferTasks(batch *gocql.Batch, transferTasks []Task, domainID, workflowID,
	runID string, cqlNowTimestamp int64) {
	targetDomainID := domainID
	for _, task := range transferTasks {
		var taskList string
		var scheduleID int64
		targetWorkflowID := transferTaskTransferTargetWorkflowID
		targetRunID := transferTaskTypeTransferTargetRunID
		targetChildWorkflowOnly := false

		switch task.GetType() {
		case TransferTaskTypeActivityTask:
			targetDomainID = task.(*ActivityTask).DomainID
			taskList = task.(*ActivityTask).TaskList
			scheduleID = task.(*ActivityTask).ScheduleID

		case TransferTaskTypeDecisionTask:
			targetDomainID = task.(*DecisionTask).DomainID
			taskList = task.(*DecisionTask).TaskList
			scheduleID = task.(*DecisionTask).ScheduleID

		case TransferTaskTypeCancelExecution:
			targetDomainID = task.(*CancelExecutionTask).TargetDomainID
			targetWorkflowID = task.(*CancelExecutionTask).TargetWorkflowID
			targetRunID = task.(*CancelExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = transferTaskTypeTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*CancelExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*CancelExecutionTask).InitiatedID

		case TransferTaskTypeSignalExecution:
			targetDomainID = task.(*SignalExecutionTask).TargetDomainID
			targetWorkflowID = task.(*SignalExecutionTask).TargetWorkflowID
			targetRunID = task.(*SignalExecutionTask).TargetRunID
			if targetRunID == "" {
				targetRunID = transferTaskTypeTransferTargetRunID
			}
			targetChildWorkflowOnly = task.(*SignalExecutionTask).TargetChildWorkflowOnly
			scheduleID = task.(*SignalExecutionTask).InitiatedID

		case TransferTaskTypeStartChildExecution:
			targetDomainID = task.(*StartChildExecutionTask).TargetDomainID
			targetWorkflowID = task.(*StartChildExecutionTask).TargetWorkflowID
			scheduleID = task.(*StartChildExecutionTask).InitiatedID
		}

		batch.Query(templateCreateTransferTaskQuery,
			d.shardID,
			rowTypeTransferTask,
			rowTypeTransferDomainID,
			rowTypeTransferWorkflowID,
			rowTypeTransferRunID,
			domainID,
			workflowID,
			runID,
			task.GetTaskID(),
			targetDomainID,
			targetWorkflowID,
			targetRunID,
			targetChildWorkflowOnly,
			taskList,
			task.GetType(),
			scheduleID,
			defaultVisibilityTimestamp,
			task.GetTaskID())
	}
}

func (d *cassandraPersistence) createTimerTasks(batch *gocql.Batch, timerTasks []Task, deleteTimerTask Task,
	domainID, workflowID, runID string, cqlNowTimestamp int64) {

	for _, task := range timerTasks {
		var eventID int64
		var attempt int64

		timeoutType := 0

		switch t := task.(type) {
		case *DecisionTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
			attempt = t.ScheduleAttempt
		case *ActivityTimeoutTask:
			eventID = t.EventID
			timeoutType = t.TimeoutType
		case *UserTimerTask:
			eventID = t.EventID
		}

		ts := common.UnixNanoToCQLTimestamp(GetVisibilityTSFrom(task).UnixNano())

		batch.Query(templateCreateTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			domainID,
			workflowID,
			runID,
			ts,
			task.GetTaskID(),
			task.GetType(),
			timeoutType,
			eventID,
			attempt,
			ts,
			task.GetTaskID())
	}

	if deleteTimerTask != nil {
		ts := common.UnixNanoToCQLTimestamp(GetVisibilityTSFrom(deleteTimerTask).UnixNano())
		batch.Query(templateCompleteTimerTaskQuery,
			d.shardID,
			rowTypeTimerTask,
			rowTypeTimerDomainID,
			rowTypeTimerWorkflowID,
			rowTypeTimerRunID,
			ts,
			deleteTimerTask.GetTaskID())
	}
}

func (d *cassandraPersistence) updateActivityInfos(batch *gocql.Batch, activityInfos []*ActivityInfo, deleteInfo *int64,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, a := range activityInfos {
		batch.Query(templateUpdateActivityInfoQuery,
			a.ScheduleID,
			a.ScheduleID,
			a.ScheduledEvent,
			a.ScheduledTime,
			a.StartedID,
			a.StartedEvent,
			a.StartedTime,
			a.ActivityID,
			a.RequestID,
			a.Details,
			a.ScheduleToStartTimeout,
			a.ScheduleToCloseTimeout,
			a.StartToCloseTimeout,
			a.HeartbeatTimeout,
			a.CancelRequested,
			a.CancelRequestID,
			a.LastHeartBeatUpdatedTime,
			a.TimerTaskStatus,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	if deleteInfo != nil {
		batch.Query(templateDeleteActivityInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateTimerInfos(batch *gocql.Batch, timerInfos []*TimerInfo, deleteInfos []string,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, a := range timerInfos {
		batch.Query(templateUpdateTimerInfoQuery,
			a.TimerID,
			a.TimerID,
			a.StartedID,
			a.ExpiryTime,
			a.TaskID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	for _, t := range deleteInfos {
		batch.Query(templateDeleteTimerInfoQuery,
			t,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateChildExecutionInfos(batch *gocql.Batch, childExecutionInfos []*ChildExecutionInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range childExecutionInfos {
		batch.Query(templateUpdateChildExecutionInfoQuery,
			c.InitiatedID,
			c.InitiatedID,
			c.InitiatedEvent,
			c.StartedID,
			c.StartedEvent,
			c.CreateRequestID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for ChildInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteChildExecutionInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateRequestCancelInfos(batch *gocql.Batch, requestCancelInfos []*RequestCancelInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range requestCancelInfos {
		batch.Query(templateUpdateRequestCancelInfoQuery,
			c.InitiatedID,
			c.InitiatedID,
			c.CancelRequestID,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for RequestCancelInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteRequestCancelInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateSignalInfos(batch *gocql.Batch, signalInfos []*SignalInfo,
	deleteInfo *int64, domainID, workflowID, runID string, condition int64, rangeID int64) {

	for _, c := range signalInfos {
		batch.Query(templateUpdateSignalInfoQuery,
			c.InitiatedID,
			c.InitiatedID,
			c.SignalRequestID,
			c.SignalName,
			c.Input,
			c.Control,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	// deleteInfo is the initiatedID for SignalInfo being deleted
	if deleteInfo != nil {
		batch.Query(templateDeleteSignalInfoQuery,
			*deleteInfo,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateSignalsRequested(batch *gocql.Batch, signalReqIDs []string, deleteSignalReqID string,
	domainID, workflowID, runID string, condition int64, rangeID int64) {

	if len(signalReqIDs) > 0 {
		batch.Query(templateUpdateSignalRequestedQuery,
			signalReqIDs,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}

	if deleteSignalReqID != "" {
		req := []string{deleteSignalReqID} // for cassandra set binding
		batch.Query(templateDeleteWorkflowExecutionSignalRequestedQuery,
			req,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func (d *cassandraPersistence) updateBufferedEvents(batch *gocql.Batch, newBufferedEvents *SerializedHistoryEventBatch,
	clearBufferedEvents bool, domainID, workflowID, runID string, condition int64, rangeID int64) {

	if clearBufferedEvents {
		batch.Query(templateDeleteBufferedEventsQuery,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	} else if newBufferedEvents != nil {
		values := make(map[string]interface{})
		values["encoding_type"] = newBufferedEvents.EncodingType
		values["version"] = newBufferedEvents.Version
		values["data"] = newBufferedEvents.Data
		newEventValues := []map[string]interface{}{values}
		batch.Query(templateAppendBufferedEventsQuery,
			newEventValues,
			d.shardID,
			rowTypeExecution,
			domainID,
			workflowID,
			runID,
			defaultVisibilityTimestamp,
			rowTypeExecutionTaskID,
			condition)
	}
}

func createShardInfo(currentCluster string, result map[string]interface{}) *ShardInfo {
	info := &ShardInfo{}
	for k, v := range result {
		switch k {
		case "shard_id":
			info.ShardID = v.(int)
		case "owner":
			info.Owner = v.(string)
		case "range_id":
			info.RangeID = v.(int64)
		case "stolen_since_renew":
			info.StolenSinceRenew = v.(int)
		case "updated_at":
			info.UpdatedAt = v.(time.Time)
		case "transfer_ack_level":
			info.TransferAckLevel = v.(int64)
		case "timer_ack_level":
			info.TimerAckLevel = v.(time.Time)
		case "cluster_transfer_ack_level":
			info.ClusterTransferAckLevel = v.(map[string]int64)
		case "cluster_timer_ack_level":
			info.ClusterTimerAckLevel = v.(map[string]time.Time)
		}
	}

	if info.ClusterTransferAckLevel == nil {
		info.ClusterTransferAckLevel = map[string]int64{
			currentCluster: info.TransferAckLevel,
		}
	}
	if info.ClusterTimerAckLevel == nil {
		info.ClusterTimerAckLevel = map[string]time.Time{
			currentCluster: info.TimerAckLevel,
		}
	}

	return info
}

func createWorkflowExecutionInfo(result map[string]interface{}) *WorkflowExecutionInfo {
	info := &WorkflowExecutionInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "parent_domain_id":
			info.ParentDomainID = v.(gocql.UUID).String()
		case "parent_workflow_id":
			info.ParentWorkflowID = v.(string)
		case "parent_run_id":
			info.ParentRunID = v.(gocql.UUID).String()
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "completion_event":
			info.CompletionEvent = v.([]byte)
		case "task_list":
			info.TaskList = v.(string)
		case "workflow_type_name":
			info.WorkflowTypeName = v.(string)
		case "workflow_timeout":
			info.WorkflowTimeout = int32(v.(int))
		case "decision_task_timeout":
			info.DecisionTimeoutValue = int32(v.(int))
		case "execution_context":
			info.ExecutionContext = v.([]byte)
		case "state":
			info.State = v.(int)
		case "close_status":
			info.CloseStatus = v.(int)
		case "last_first_event_id":
			info.LastFirstEventID = v.(int64)
		case "next_event_id":
			info.NextEventID = v.(int64)
		case "last_processed_event":
			info.LastProcessedEvent = v.(int64)
		case "start_time":
			info.StartTimestamp = v.(time.Time)
		case "last_updated_time":
			info.LastUpdatedTimestamp = v.(time.Time)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		case "decision_schedule_id":
			info.DecisionScheduleID = v.(int64)
		case "decision_started_id":
			info.DecisionStartedID = v.(int64)
		case "decision_request_id":
			info.DecisionRequestID = v.(string)
		case "decision_timeout":
			info.DecisionTimeout = int32(v.(int))
		case "decision_attempt":
			info.DecisionAttempt = v.(int64)
		case "decision_timestamp":
			info.DecisionTimestamp = v.(int64)
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		case "sticky_task_list":
			info.StickyTaskList = v.(string)
		case "sticky_schedule_to_start_timeout":
			info.StickyScheduleToStartTimeout = int32(v.(int))
		case "client_library_version":
			info.ClientLibraryVersion = v.(string)
		case "client_feature_version":
			info.ClientFeatureVersion = v.(string)
		case "client_impl":
			info.ClientImpl = v.(string)
		}
	}

	return info
}

func createTransferTaskInfo(result map[string]interface{}) *TransferTaskInfo {
	info := &TransferTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "task_id":
			info.TaskID = v.(int64)
		case "target_domain_id":
			info.TargetDomainID = v.(gocql.UUID).String()
		case "target_workflow_id":
			info.TargetWorkflowID = v.(string)
		case "target_run_id":
			info.TargetRunID = v.(gocql.UUID).String()
			if info.TargetRunID == transferTaskTypeTransferTargetRunID {
				info.TargetRunID = ""
			}
		case "target_child_workflow_only":
			info.TargetChildWorkflowOnly = v.(bool)
		case "task_list":
			info.TaskList = v.(string)
		case "type":
			info.TaskType = v.(int)
		case "schedule_id":
			info.ScheduleID = v.(int64)
		}
	}

	return info
}

func createActivityInfo(result map[string]interface{}) *ActivityInfo {
	info := &ActivityInfo{}
	for k, v := range result {
		switch k {
		case "schedule_id":
			info.ScheduleID = v.(int64)
		case "scheduled_event":
			info.ScheduledEvent = v.([]byte)
		case "scheduled_time":
			info.ScheduledTime = v.(time.Time)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			info.StartedEvent = v.([]byte)
		case "started_time":
			info.StartedTime = v.(time.Time)
		case "activity_id":
			info.ActivityID = v.(string)
		case "request_id":
			info.RequestID = v.(string)
		case "details":
			info.Details = v.([]byte)
		case "schedule_to_start_timeout":
			info.ScheduleToStartTimeout = int32(v.(int))
		case "schedule_to_close_timeout":
			info.ScheduleToCloseTimeout = int32(v.(int))
		case "start_to_close_timeout":
			info.StartToCloseTimeout = int32(v.(int))
		case "heart_beat_timeout":
			info.HeartbeatTimeout = int32(v.(int))
		case "cancel_requested":
			info.CancelRequested = v.(bool)
		case "cancel_request_id":
			info.CancelRequestID = v.(int64)
		case "last_hb_updated_time":
			info.LastHeartBeatUpdatedTime = v.(time.Time)
		case "timer_task_status":
			info.TimerTaskStatus = int32(v.(int))
		}
	}

	return info
}

func createTimerInfo(result map[string]interface{}) *TimerInfo {
	info := &TimerInfo{}
	for k, v := range result {
		switch k {
		case "timer_id":
			info.TimerID = v.(string)
		case "started_id":
			info.StartedID = v.(int64)
		case "expiry_time":
			info.ExpiryTime = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		}
	}
	return info
}

func createChildExecutionInfo(result map[string]interface{}) *ChildExecutionInfo {
	info := &ChildExecutionInfo{}
	for k, v := range result {
		switch k {
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "initiated_event":
			info.InitiatedEvent = v.([]byte)
		case "started_id":
			info.StartedID = v.(int64)
		case "started_event":
			info.StartedEvent = v.([]byte)
		case "create_request_id":
			info.CreateRequestID = v.(gocql.UUID).String()
		}
	}

	return info
}

func createRequestCancelInfo(result map[string]interface{}) *RequestCancelInfo {
	info := &RequestCancelInfo{}
	for k, v := range result {
		switch k {
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "cancel_request_id":
			info.CancelRequestID = v.(string)
		}
	}

	return info
}

func createSignalInfo(result map[string]interface{}) *SignalInfo {
	info := &SignalInfo{}
	for k, v := range result {
		switch k {
		case "initiated_id":
			info.InitiatedID = v.(int64)
		case "signal_request_id":
			info.SignalRequestID = v.(gocql.UUID).String()
		case "signal_name":
			info.SignalName = v.(string)
		case "input":
			info.Input = v.([]byte)
		case "control":
			info.Control = v.([]byte)
		}
	}

	return info
}

func createSerializedHistoryEventBatch(result map[string]interface{}) *SerializedHistoryEventBatch {
	// TODO: default to JSON, update this when we support different encoding types.
	eventBatch := &SerializedHistoryEventBatch{EncodingType: common.EncodingTypeJSON}
	for k, v := range result {
		switch k {
		case "version":
			eventBatch.Version = v.(int)
		case "data":
			eventBatch.Data = v.([]byte)
		}
	}

	return eventBatch
}

func createTaskInfo(result map[string]interface{}) *TaskInfo {
	info := &TaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "schedule_id":
			info.ScheduleID = v.(int64)
		}
	}

	return info
}

func createTimerTaskInfo(result map[string]interface{}) *TimerTaskInfo {
	info := &TimerTaskInfo{}
	for k, v := range result {
		switch k {
		case "domain_id":
			info.DomainID = v.(gocql.UUID).String()
		case "workflow_id":
			info.WorkflowID = v.(string)
		case "run_id":
			info.RunID = v.(gocql.UUID).String()
		case "visibility_ts":
			info.VisibilityTimestamp = v.(time.Time)
		case "task_id":
			info.TaskID = v.(int64)
		case "type":
			info.TaskType = v.(int)
		case "timeout_type":
			info.TimeoutType = v.(int)
		case "event_id":
			info.EventID = v.(int64)
		case "schedule_attempt":
			info.ScheduleAttempt = v.(int64)
		}
	}

	return info
}

func isTimeoutError(err error) bool {
	if err == gocql.ErrTimeoutNoResponse {
		return true
	}
	if err == gocql.ErrConnectionClosed {
		return true
	}
	_, ok := err.(*gocql.RequestErrWriteTimeout)
	return ok
}

func isThrottlingError(err error) bool {
	if req, ok := err.(gocql.RequestError); ok {
		// gocql does not expose the constant errOverloaded = 0x1001
		return req.Code() == 0x1001
	}
	return false
}

// GetVisibilityTSFrom - helper method to get visibility timestamp
func GetVisibilityTSFrom(task Task) time.Time {
	switch task.GetType() {
	case TaskTypeDecisionTimeout:
		return task.(*DecisionTimeoutTask).VisibilityTimestamp

	case TaskTypeActivityTimeout:
		return task.(*ActivityTimeoutTask).VisibilityTimestamp

	case TaskTypeUserTimer:
		return task.(*UserTimerTask).VisibilityTimestamp

	case TaskTypeWorkflowTimeout:
		return task.(*WorkflowTimeoutTask).VisibilityTimestamp

	case TaskTypeDeleteHistoryEvent:
		return task.(*DeleteHistoryEventTask).VisibilityTimestamp
	}
	return time.Time{}
}

// SetVisibilityTSFrom - helper method to set visibility timestamp
func SetVisibilityTSFrom(task Task, t time.Time) {
	switch task.GetType() {
	case TaskTypeDecisionTimeout:
		task.(*DecisionTimeoutTask).VisibilityTimestamp = t

	case TaskTypeActivityTimeout:
		task.(*ActivityTimeoutTask).VisibilityTimestamp = t

	case TaskTypeUserTimer:
		task.(*UserTimerTask).VisibilityTimestamp = t

	case TaskTypeWorkflowTimeout:
		task.(*WorkflowTimeoutTask).VisibilityTimestamp = t

	case TaskTypeDeleteHistoryEvent:
		task.(*DeleteHistoryEventTask).VisibilityTimestamp = t
	}
}
