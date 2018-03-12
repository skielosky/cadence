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

package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/pborman/uuid"
	"github.com/uber/cadence/common"
	"github.com/urfave/cli"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	s "go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
)

/**
Flags used to specify cli command line arguments
*/
const (
	FlagAddress                   = "address"
	FlagAddressWithAlias          = FlagAddress + ", ad"
	FlagDomain                    = "domain"
	FlagDomainWithAlias           = FlagDomain + ", do"
	FlagWorkflowID                = "workflow_id"
	FlagWorkflowIDWithAlias       = FlagWorkflowID + ", wid, w"
	FlagRunID                     = "run_id"
	FlagRunIDWithAlias            = FlagRunID + ", rid, r"
	FlagTaskList                  = "tasklist"
	FlagTaskListWithAlias         = FlagTaskList + ", tl"
	FlagTaskListType              = "tasklisttype"
	FlagTaskListTypeWithAlias     = FlagTaskListType + ", tlt"
	FlagWorkflowType              = "workflow_type"
	FlagWorkflowTypeWithAlias     = FlagWorkflowType + ", wt"
	FlagExecutionTimeout          = "execution_timeout"
	FlagExecutionTimeoutWithAlias = FlagExecutionTimeout + ", et"
	FlagDecisionTimeout           = "decision_timeout"
	FlagDecisionTimeoutWithAlias  = FlagDecisionTimeout + ", dt"
	FlagContextTimeout            = "context_timeout"
	FlagContextTimeoutWithAlias   = FlagContextTimeout + ", ct"
	FlagInput                     = "input"
	FlagInputWithAlias            = FlagInput + ", i"
	FlagInputFile                 = "input_file"
	FlagInputFileWithAlias        = FlagInputFile + ", if"
	FlagReason                    = "reason"
	FlagReasonWithAlias           = FlagReason + ", re"
	FlagOpen                      = "open"
	FlagOpenWithAlias             = FlagOpen + ", op"
	FlagPageSize                  = "pagesize"
	FlagPageSizeWithAlias         = FlagPageSize + ", ps"
	FlagEarliestTime              = "earliest_time"
	FlagEarliestTimeWithAlias     = FlagEarliestTime + ", et"
	FlagLatestTime                = "latest_time"
	FlagLatestTimeWithAlias       = FlagLatestTime + ", lt"
	FlagPrintRawTime              = "print_raw_time"
	FlagPrintRawTimeWithAlias     = FlagPrintRawTime + ", prt"
	FlagPrintDateTime             = "print_datetime"
	FlagPrintDateTimeWithAlias    = FlagPrintDateTime + ", pdt"
	FlagDescription               = "description"
	FlagDescriptionWithAlias      = FlagDescription + ", desc"
	FlagOwnerEmail                = "owner_email"
	FlagOwnerEmailWithAlias       = FlagOwnerEmail + ", oe"
	FlagRetentionDays             = "retention_days"
	FlagRetentionDaysWithAlias    = FlagRetentionDays + ", rd"
	FlagEmitMetric                = "emit_metric"
	FlagEmitMetricWithAlias       = FlagEmitMetric + ", em"
	FlagName                      = "name"
	FlagNameWithAlias             = FlagName + ", n"
	FlagOutputFilename            = "output_filename"
	FlagOutputFilenameWithAlias   = FlagOutputFilename + ", of"
	FlagQueryType                 = "query_type"
	FlagQueryTypeWithAlias        = FlagQueryType + ", qt"
)

const (
	localHostPort = "127.0.0.1:7933"

	maxOutputStringLength = 200 // max length for output string

	defaultTimeFormat                = time.RFC3339 // used for converting UnixNano to string like 2018-02-15T16:16:36-08:00
	defaultDomainRetentionDays       = 3
	defaultContextTimeoutForLongPoll = 2 * time.Minute
	defaultDecisionTimeoutInSeconds  = 10
)

// For color output to terminal
var (
	colorRed     = color.New(color.FgRed).SprintFunc()
	colorMagenta = color.New(color.FgMagenta).SprintFunc()
	colorGreen   = color.New(color.FgGreen).SprintFunc()
)

// cBuilder is used to create cadence clients
// To provide customized builder, call SetBuilder() before call NewCliApp()
var cBuilder WorkflowClientBuilderInterface

// SetBuilder can be used to inject customized builder of cadence clients
func SetBuilder(builder WorkflowClientBuilderInterface) {
	cBuilder = builder
}

// ErrorAndExit print easy to understand error msg first then error detail in a new line
func ErrorAndExit(msg string, err error) {
	fmt.Printf("%s %s\n%s %+v\n", colorRed("Error:"), msg, colorMagenta("Error Details:"), err)
	os.Exit(1)
}

// ExitIfError exit while err is not nil and print the calling stack also
func ExitIfError(err error) {
	const stacksEnv = `CADENCE_CLI_SHOW_STACKS`
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		if os.Getenv(stacksEnv) != `` {
			debug.PrintStack()
		} else {
			fmt.Fprintf(os.Stderr, "('export %s=1' to see stack traces)\n", stacksEnv)
		}
		os.Exit(1)
	}
}

// RegisterDomain register a domain
func RegisterDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	description := c.String(FlagDescription)
	ownerEmail := c.String(FlagOwnerEmail)
	retentionDays := defaultDomainRetentionDays
	if c.IsSet(FlagRetentionDays) {
		retentionDays = c.Int(FlagRetentionDays)
	}
	emitMetric := false
	var err error
	if c.IsSet(FlagEmitMetric) {
		emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
		if err != nil {
			fmt.Printf("Register Domain failed: %v.\n", err.Error())
			return
		}
	}

	request := &s.RegisterDomainRequest{
		Name:                                   common.StringPtr(domain),
		Description:                            common.StringPtr(description),
		OwnerEmail:                             common.StringPtr(ownerEmail),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
	}

	ctx, cancel := newContext()
	defer cancel()
	err = domainClient.Register(ctx, request)
	if err != nil {
		if _, ok := err.(*s.DomainAlreadyExistsError); !ok {
			fmt.Printf("Operation failed: %v.\n", err.Error())
		} else {
			fmt.Printf("Domain %s already registered.\n", domain)
		}
	} else {
		fmt.Printf("Domain %s succeesfully registered.\n", domain)
	}
}

// UpdateDomain updates a domain
func UpdateDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	ctx, cancel := newContext()
	defer cancel()
	info, config, err := domainClient.Describe(ctx, domain)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			fmt.Printf("Operation failed: %v.\n", err.Error())
		} else {
			fmt.Printf("Domain %s does not exist.\n", domain)
		}
	}

	description := info.GetDescription()
	if c.IsSet(FlagDescription) {
		description = c.String(FlagDescription)
	}
	ownerEmail := info.GetOwnerEmail()
	if c.IsSet(FlagOwnerEmail) {
		ownerEmail = c.String(FlagOwnerEmail)
	}
	retentionDays := config.GetWorkflowExecutionRetentionPeriodInDays()
	if c.IsSet(FlagRetentionDays) {
		retentionDays = int32(c.Int(FlagRetentionDays))
	}
	emitMetric := config.GetEmitMetric()
	if c.IsSet(FlagEmitMetric) {
		emitMetric, err = strconv.ParseBool(c.String(FlagEmitMetric))
		if err != nil {
			ErrorAndExit("Update Domain failed", err)
		}
	}

	updateInfo := &s.UpdateDomainInfo{
		Description: common.StringPtr(description),
		OwnerEmail:  common.StringPtr(ownerEmail),
	}
	updateConfig := &s.DomainConfiguration{
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(int32(retentionDays)),
		EmitMetric:                             common.BoolPtr(emitMetric),
	}

	err = domainClient.Update(ctx, domain, updateInfo, updateConfig)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			fmt.Printf("Operation failed: %v.\n", err.Error())
		} else {
			fmt.Printf("Domain %s does not exist.\n", domain)
		}
	} else {
		fmt.Printf("Domain %s succeesfully updated.\n", domain)
	}
}

// DescribeDomain updates a domain
func DescribeDomain(c *cli.Context) {
	domainClient := getDomainClient(c)
	domain := getRequiredGlobalOption(c, FlagDomain)

	ctx, cancel := newContext()
	defer cancel()
	info, config, err := domainClient.Describe(ctx, domain)
	if err != nil {
		if _, ok := err.(*s.EntityNotExistsError); !ok {
			fmt.Printf("Operation failed: %v.\n", err.Error())
		} else {
			fmt.Printf("Domain %s does not exist.\n", domain)
		}
	} else {
		fmt.Printf("Name:%v, Description:%v, OwnerEmail:%v, Status:%v, RetentionInDays:%v, EmitMetrics:%v\n",
			info.GetName(),
			info.GetDescription(),
			info.GetOwnerEmail(),
			info.GetStatus(),
			config.GetWorkflowExecutionRetentionPeriodInDays(),
			config.GetEmitMetric())
	}
}

// ShowHistory shows the history of given workflow execution based on workflowID and runID.
func ShowHistory(c *cli.Context) {
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	showHistoryHelper(c, wid, rid)
}

// ShowHistoryWithWID shows the history of given workflow with workflow_id
func ShowHistoryWithWID(c *cli.Context) {
	if !c.Args().Present() {
		ExitIfError(errors.New("workflow_id is required"))
	}
	wid := c.Args().First()
	rid := ""
	if c.NArg() >= 2 {
		rid = c.Args().Get(1)
	}
	showHistoryHelper(c, wid, rid)
}

func showHistoryHelper(c *cli.Context, wid, rid string) {
	wfClient := getWorkflowClient(c)

	printDateTime := c.Bool(FlagPrintDateTime)
	printRawTime := c.Bool(FlagPrintRawTime)
	outputFileName := c.String(FlagOutputFilename)

	ctx, cancel := newContext()
	defer cancel()
	history, err := GetHistory(ctx, wfClient, wid, rid)
	if err != nil {
		ExitIfError(err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("")
	for _, e := range history.Events {
		if printRawTime {
			table.Append([]string{strconv.FormatInt(e.GetEventId(), 10), strconv.FormatInt(e.GetTimestamp(), 10), ColorEvent(e), HistoryEventToString(e)})
		} else if printDateTime {
			table.Append([]string{strconv.FormatInt(e.GetEventId(), 10), convertTime(e.GetTimestamp()), ColorEvent(e), HistoryEventToString(e)})
		} else {
			table.Append([]string{strconv.FormatInt(e.GetEventId(), 10), ColorEvent(e), HistoryEventToString(e)})
		}
	}
	table.Render()

	if outputFileName != "" {
		serializer := &JSONHistorySerializer{}
		data, err := serializer.Serialize(history)
		if err != nil {
			ExitIfError(err)
		}
		if err := ioutil.WriteFile(outputFileName, data, 0777); err != nil {
			ExitIfError(err)
		}
	}
}

// StartWorkflow starts a new workflow execution
func StartWorkflow(c *cli.Context) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	tasklist := getRequiredOption(c, FlagTaskList)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagExecutionTimeout)
	if et == 0 {
		ExitIfError(errors.New(FlagExecutionTimeout + " is required"))
	}
	dt := c.Int(FlagDecisionTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}

	input := processJSONInput(c)

	tcCtx, cancel := newContext()
	defer cancel()

	resp, err := serviceClient.StartWorkflowExecution(tcCtx, &s.StartWorkflowExecutionRequest{
		RequestId:  common.StringPtr(uuid.New()),
		Domain:     common.StringPtr(domain),
		WorkflowId: common.StringPtr(wid),
		WorkflowType: &s.WorkflowType{
			Name: common.StringPtr(workflowType),
		},
		TaskList: &s.TaskList{
			Name: common.StringPtr(tasklist),
		},
		Input: []byte(input),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(int32(et)),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(int32(dt)),
		Identity:                            common.StringPtr(getCliIdentity()),
	})

	if err != nil {
		ErrorAndExit("Failed to create workflow", err)
	} else {
		fmt.Printf("Started Workflow Id: %s, run Id: %s\n", wid, resp.GetRunId())
	}
}

// RunWorkflow starts a new workflow execution and print workflow progress and result
func RunWorkflow(c *cli.Context) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	tasklist := getRequiredOption(c, FlagTaskList)
	workflowType := getRequiredOption(c, FlagWorkflowType)
	et := c.Int(FlagExecutionTimeout)
	if et == 0 {
		ExitIfError(errors.New(FlagExecutionTimeout + " is required"))
	}
	dt := c.Int(FlagDecisionTimeout)
	wid := c.String(FlagWorkflowID)
	if len(wid) == 0 {
		wid = uuid.New()
	}

	input := processJSONInput(c)

	contextTimeout := defaultContextTimeoutForLongPoll
	if c.IsSet(FlagContextTimeout) {
		contextTimeout = time.Duration(c.Int(FlagContextTimeout)) * time.Second
	}
	tcCtx, cancel := newContextForLongPoll(contextTimeout)
	defer cancel()

	resp, err := serviceClient.StartWorkflowExecution(tcCtx, &s.StartWorkflowExecutionRequest{
		RequestId:  common.StringPtr(uuid.New()),
		Domain:     common.StringPtr(domain),
		WorkflowId: common.StringPtr(wid),
		WorkflowType: &s.WorkflowType{
			Name: common.StringPtr(workflowType),
		},
		TaskList: &s.TaskList{
			Name: common.StringPtr(tasklist),
		},
		Input: []byte(input),
		ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(int32(et)),
		TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(int32(dt)),
		Identity:                            common.StringPtr(getCliIdentity()),
	})

	if err != nil {
		ErrorAndExit("Failed to create workflow", err)
	}

	// print execution summary
	fmt.Println(colorMagenta("Running execution:"))
	table := tablewriter.NewWriter(os.Stdout)
	executionData := [][]string{
		{"Workflow Id", wid},
		{"Run Id", resp.GetRunId()},
		{"Type", workflowType},
		{"Domain", domain},
		{"Task List", tasklist},
		{"Args", truncate(input)}, // in case of large input
	}
	table.SetBorder(false)
	table.SetColumnSeparator(":")
	table.AppendBulk(executionData) // Add Bulk Data
	table.Render()

	// print workflow progress
	fmt.Println(colorMagenta("Progress:"))
	wfClient := getWorkflowClient(c)
	timeElapse := 1
	isTimeElapseExist := false
	doneChan := make(chan bool)
	var lastEvent *s.HistoryEvent // used for print result of this run
	ticker := time.NewTicker(time.Second).C

	go func() {
		iter := wfClient.GetWorkflowHistory(tcCtx, wid, resp.GetRunId(), true, s.HistoryEventFilterTypeAllEvent)
		for iter.HasNext() {
			event, err := iter.Next()
			if err != nil {
				ErrorAndExit("Unable to read event", err)
			}
			if isTimeElapseExist {
				removePrevious2LinesFromTerminal()
				isTimeElapseExist = false
			}
			fmt.Printf("  %d, %s, %v\n", event.GetEventId(), convertTime(event.GetTimestamp()), event.GetEventType())
			lastEvent = event
		}
		doneChan <- true
	}()

	for {
		select {
		case <-ticker:
			if isTimeElapseExist {
				removePrevious2LinesFromTerminal()
			}
			fmt.Printf("\nTime elapse: %ds\n", timeElapse)
			isTimeElapseExist = true
			timeElapse++
		case <-doneChan: // print result of this run
			fmt.Println(colorMagenta("\nResult:"))
			fmt.Printf("  Run Time: %d seconds\n", timeElapse)
			printRunStatus(lastEvent)
			return
		}
	}
}

// TerminateWorkflow terminates a workflow execution
func TerminateWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	reason := c.String(FlagReason)

	ctx, cancel := newContext()
	defer cancel()
	err := wfClient.TerminateWorkflow(ctx, wid, rid, reason, nil)

	if err != nil {
		ErrorAndExit("Terminate workflow failed", err)
	} else {
		fmt.Println("Terminate workflow succeed.")
	}
}

// CancelWorkflow cancels a workflow execution
func CancelWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)

	ctx, cancel := newContext()
	defer cancel()
	err := wfClient.CancelWorkflow(ctx, wid, rid)

	if err != nil {
		ErrorAndExit("Cancel workflow failed", err)
	} else {
		fmt.Println("Cancel workflow succeed.")
	}
}

// SignalWorkflow signals a workflow execution
func SignalWorkflow(c *cli.Context) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	name := getRequiredOption(c, FlagName)
	input := processJSONInput(c)

	tcCtx, cancel := newContext()
	defer cancel()
	err := serviceClient.SignalWorkflowExecution(tcCtx, &s.SignalWorkflowExecutionRequest{
		Domain: common.StringPtr(domain),
		WorkflowExecution: &s.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      getPtrOrNilIfEmpty(rid),
		},
		SignalName: common.StringPtr(name),
		Input:      []byte(input),
		Identity:   common.StringPtr(getCliIdentity()),
	})

	if err != nil {
		ErrorAndExit("Signal workflow failed", err)
	} else {
		fmt.Println("Signal workflow succeed.")
	}
}

// QueryWorkflow query workflow execution
func QueryWorkflow(c *cli.Context) {
	getRequiredGlobalOption(c, FlagDomain) // for pre-check and alert if not provided
	getRequiredOption(c, FlagWorkflowID)
	queryType := getRequiredOption(c, FlagQueryType)

	queryWorkflowHelper(c, queryType)
}

// QueryWorkflowUsingStackTrace query workflow execution using __stack_trace as query type
func QueryWorkflowUsingStackTrace(c *cli.Context) {
	queryWorkflowHelper(c, "__stack_trace")
}

func queryWorkflowHelper(c *cli.Context, queryType string) {
	// using service client instead of cadence.Client because we need to directly pass the json blob as input.
	serviceClient := getWorkflowServiceClient(c)

	domain := getRequiredGlobalOption(c, FlagDomain)
	wid := getRequiredOption(c, FlagWorkflowID)
	rid := c.String(FlagRunID)
	input := processJSONInput(c)

	tcCtx, cancel := newContext()
	defer cancel()
	queryRequest := &s.QueryWorkflowRequest{
		Domain: common.StringPtr(domain),
		Execution: &s.WorkflowExecution{
			WorkflowId: common.StringPtr(wid),
			RunId:      getPtrOrNilIfEmpty(rid),
		},
		Query: &s.WorkflowQuery{
			QueryType: common.StringPtr(queryType),
		},
	}
	if input != "" {
		queryRequest.Query.QueryArgs = []byte(input)
	}
	queryResponse, err := serviceClient.QueryWorkflow(tcCtx, queryRequest)
	if err != nil {
		ErrorAndExit("Query failed", err)
		return
	}

	// assume it is json encoded
	fmt.Printf("Query result as JSON:\n%v\n", string(queryResponse.QueryResult))
}

// ListWorkflow list workflow executions based on filters
func ListWorkflow(c *cli.Context) {
	wfClient := getWorkflowClient(c)

	queryOpen := c.Bool(FlagOpen)
	pageSize := c.Int(FlagPageSize)
	earliestTime := parseTime(c.String(FlagEarliestTime), 0)
	latestTime := parseTime(c.String(FlagLatestTime), time.Now().UnixNano())
	workflowID := c.String(FlagWorkflowID)
	workflowType := c.String(FlagWorkflowType)
	printRawTime := c.Bool(FlagPrintRawTime)

	if len(workflowID) > 0 && len(workflowType) > 0 {
		ExitIfError(errors.New("you can filter on workflow_id or workflow_type, but not on both"))
	}

	reader := bufio.NewReader(os.Stdin)
	var result []*s.WorkflowExecutionInfo
	var nextPageToken []byte
	for {
		if queryOpen {
			result, nextPageToken = listOpenWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, nextPageToken)
		} else {
			result, nextPageToken = listClosedWorkflow(wfClient, pageSize, earliestTime, latestTime, workflowID, workflowType, nextPageToken)
		}

		for _, e := range result {
			fmt.Printf("%s, -w %s -r %s", e.Type.GetName(), e.Execution.GetWorkflowId(), e.Execution.GetRunId())
			if printRawTime {
				fmt.Printf(" [%d, %d]\n", e.GetStartTime(), e.GetCloseTime())
			} else {
				fmt.Printf(" [%s, %s]\n", convertTime(e.GetStartTime()), convertTime(e.GetCloseTime()))
			}
		}

		if len(result) < pageSize {
			break
		}

		fmt.Println("Press C then Enter to show more result, press any other key then Enter to quit: ")
		input, _ := reader.ReadString('\n')
		c := []byte(input)[0]
		if c == 'C' || c == 'c' {
			continue
		} else {
			break
		}
	}
}

func listOpenWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string, nextPageToken []byte) ([]*s.WorkflowExecutionInfo, []byte) {
	request := &s.ListOpenWorkflowExecutionsRequest{
		MaximumPageSize: common.Int32Ptr(int32(pageSize)),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &s.StartTimeFilter{
			EarliestTime: common.Int64Ptr(earliestTime),
			LatestTime:   common.Int64Ptr(latestTime),
		},
	}
	if len(workflowID) > 0 {
		request.ExecutionFilter = &s.WorkflowExecutionFilter{WorkflowId: common.StringPtr(workflowID)}
	}
	if len(workflowType) > 0 {
		request.TypeFilter = &s.WorkflowTypeFilter{Name: common.StringPtr(workflowType)}
	}

	ctx, cancel := newContext()
	defer cancel()
	response, err := client.ListOpenWorkflow(ctx, request)
	if err != nil {
		ExitIfError(err)
	}
	return response.Executions, response.NextPageToken
}

func listClosedWorkflow(client client.Client, pageSize int, earliestTime, latestTime int64, workflowID, workflowType string, nextPageToken []byte) ([]*s.WorkflowExecutionInfo, []byte) {
	request := &s.ListClosedWorkflowExecutionsRequest{
		MaximumPageSize: common.Int32Ptr(int32(pageSize)),
		NextPageToken:   nextPageToken,
		StartTimeFilter: &s.StartTimeFilter{
			EarliestTime: common.Int64Ptr(earliestTime),
			LatestTime:   common.Int64Ptr(latestTime),
		},
	}
	if len(workflowID) > 0 {
		request.ExecutionFilter = &s.WorkflowExecutionFilter{WorkflowId: common.StringPtr(workflowID)}
	}
	if len(workflowType) > 0 {
		request.TypeFilter = &s.WorkflowTypeFilter{Name: common.StringPtr(workflowType)}
	}

	ctx, cancel := newContext()
	defer cancel()
	response, err := client.ListClosedWorkflow(ctx, request)
	if err != nil {
		ExitIfError(err)
	}
	return response.Executions, response.NextPageToken
}

// DescribeTaskList show pollers info of a given tasklist
func DescribeTaskList(c *cli.Context) {
	wfClient := getWorkflowClient(c)
	taskList := getRequiredOption(c, FlagTaskList)
	taskListType := strToTaskListType(c.String(FlagTaskListType)) // default type is decision

	ctx, cancel := newContext()
	defer cancel()
	response, err := wfClient.DescribeTaskList(ctx, taskList, taskListType)
	if err != nil {
		ErrorAndExit("DescribeTaskList failed", err)
	}

	pollers := response.Pollers
	if len(pollers) == 0 {
		fmt.Println(colorMagenta("No poller for tasklist: " + taskList))
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetBorder(false)
	table.SetColumnSeparator("|")
	table.SetHeader([]string{"Poller Identity", "Last Access Time"})
	for _, poller := range pollers {
		table.Append([]string{poller.GetIdentity(), convertTime(poller.GetLastAccessTime())})
	}
	table.Render()
}

func getDomainClient(c *cli.Context) client.DomainClient {
	service, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ExitIfError(err)
	}

	domainClient, err := client.NewDomainClient(service, &client.Options{}), nil
	if err != nil {
		ExitIfError(err)
	}
	return domainClient
}

func getWorkflowClient(c *cli.Context) client.Client {
	domain := getRequiredGlobalOption(c, FlagDomain)

	service, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ExitIfError(err)
	}

	wfClient, err := client.NewClient(service, domain, &client.Options{}), nil
	if err != nil {
		ExitIfError(err)
	}

	return wfClient
}

func getWorkflowServiceClient(c *cli.Context) workflowserviceclient.Interface {
	client, err := cBuilder.BuildServiceClient(c)
	if err != nil {
		ExitIfError(err)
	}

	return client
}

func getRequiredOption(c *cli.Context, optionName string) string {
	value := c.String(optionName)
	if len(value) == 0 {
		ExitIfError(fmt.Errorf("%s is required", optionName))
	}
	return value
}

func getRequiredGlobalOption(c *cli.Context, optionName string) string {
	value := c.GlobalString(optionName)
	if len(value) == 0 {
		ExitIfError(fmt.Errorf("%s is required", optionName))
	}
	return value
}

func convertTime(unixNano int64) string {
	t2 := time.Unix(0, unixNano)
	return t2.Format(defaultTimeFormat)
}

func parseTime(timeStr string, defaultValue int64) int64 {
	if len(timeStr) == 0 {
		return defaultValue
	}

	// try to parse
	parsedTime, err := time.Parse(defaultTimeFormat, timeStr)
	if err == nil {
		return parsedTime.UnixNano()
	}

	// treat as raw time
	resultValue, err := strconv.ParseInt(timeStr, 10, 64)
	if err != nil {
		ExitIfError(fmt.Errorf("cannot parse time '%s', use UTC format '2006-01-02T15:04:05Z' or raw UnixNano directly. Error: %v", timeStr, err))
	}

	return resultValue
}

func strToTaskListType(str string) s.TaskListType {
	if strings.ToLower(str) == "activity" {
		return s.TaskListTypeActivity
	}
	return s.TaskListTypeDecision
}

func getPtrOrNilIfEmpty(value string) *string {
	if value == "" {
		return nil
	}
	return common.StringPtr(value)
}

func getCliIdentity() string {
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "UnKnown"
	}
	return fmt.Sprintf("cadence-cli@%s", hostName)
}

func newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Second*5)
}

func newContextForLongPoll(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

// process and validate input provided through cmd or file
func processJSONInput(c *cli.Context) string {
	var input string
	if c.IsSet(FlagInput) {
		input = c.String(FlagInput)
	} else if c.IsSet(FlagInputFile) {
		inputFile := c.String(FlagInputFile)
		data, err := ioutil.ReadFile(inputFile)
		if err != nil {
			ErrorAndExit("Error reading input file", err)
		}
		input = string(data)
	}
	if input != "" {
		if err := validateJSONs(input); err != nil {
			ErrorAndExit("Input is not valid JSON, or JSONs concatenated with spaces/newlines.", err)
		}
	}
	return input
}

// validate whether str is a valid json or multi valid json concatenated with spaces/newlines
func validateJSONs(str string) error {
	input := []byte(str)
	dec := json.NewDecoder(bytes.NewReader(input))
	for {
		_, err := dec.Token()
		if err == io.EOF {
			return nil // End of input, valid JSON
		}
		if err != nil {
			return err // Invalid input
		}
	}
}

func truncate(str string) string {
	if len(str) > maxOutputStringLength {
		return str[:maxOutputStringLength]
	}
	return str
}

// this only works for ANSI terminal, which means remove existing lines won't work if users redirect to file
// ref: https://en.wikipedia.org/wiki/ANSI_escape_code
func removePrevious2LinesFromTerminal() {
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
	fmt.Printf("\033[1A")
	fmt.Printf("\033[2K")
}

func printRunStatus(event *s.HistoryEvent) {
	switch event.GetEventType() {
	case s.EventTypeWorkflowExecutionCompleted:
		fmt.Printf("  Status: %s\n", colorGreen("COMPLETED"))
		fmt.Printf("  Output: %s\n", string(event.WorkflowExecutionCompletedEventAttributes.Result))
	case s.EventTypeWorkflowExecutionFailed:
		fmt.Printf("  Status: %s\n", colorRed("FAILED"))
		fmt.Printf("  Reason: %s\n", event.WorkflowExecutionFailedEventAttributes.GetReason())
		fmt.Printf("  Detail: %s\n", string(event.WorkflowExecutionFailedEventAttributes.Details))
	case s.EventTypeWorkflowExecutionTimedOut:
		fmt.Printf("  Status: %s\n", colorRed("TIMEOUT"))
		fmt.Printf("  Timeout Type: %s\n", event.WorkflowExecutionTimedOutEventAttributes.GetTimeoutType())
	case s.EventTypeWorkflowExecutionCanceled:
		fmt.Printf("  Status: %s\n", colorRed("CANCELED"))
		fmt.Printf("  Detail: %s\n", string(event.WorkflowExecutionCanceledEventAttributes.Details))
	}
}
