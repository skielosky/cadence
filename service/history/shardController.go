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
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"

	"github.com/uber-common/bark"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	shardController struct {
		host                *membership.HostInfo
		hServiceResolver    membership.ServiceResolver
		membershipUpdateCh  chan *membership.ChangedEvent
		shardMgr            persistence.ShardManager
		historyMgr          persistence.HistoryManager
		metadataMgr         persistence.MetadataManager
		executionMgrFactory persistence.ExecutionManagerFactory
		engineFactory       EngineFactory
		shardClosedCh       chan int
		isStarted           int32
		isStopped           int32
		shutdownWG          sync.WaitGroup
		shutdownCh          chan struct{}
		logger              bark.Logger
		config              *Config
		metricsClient       metrics.Client
		clusterMetadata     cluster.Metadata

		sync.RWMutex
		historyShards map[int]*historyShardsItem
		isStopping    bool
	}

	historyShardsItem struct {
		sync.RWMutex
		shardID         int
		shardMgr        persistence.ShardManager
		historyMgr      persistence.HistoryManager
		executionMgr    persistence.ExecutionManager
		domainCache     cache.DomainCache
		engineFactory   EngineFactory
		host            *membership.HostInfo
		engine          Engine
		config          *Config
		logger          bark.Logger
		metricsClient   metrics.Client
		clusterMetadata cluster.Metadata
	}
)

func newShardController(host *membership.HostInfo, resolver membership.ServiceResolver,
	shardMgr persistence.ShardManager, historyMgr persistence.HistoryManager, metadataMgr persistence.MetadataManager,
	executionMgrFactory persistence.ExecutionManagerFactory, factory EngineFactory, config *Config,
	logger bark.Logger, metricsClient metrics.Client, clusterMetadata cluster.Metadata) *shardController {
	return &shardController{
		host:                host,
		hServiceResolver:    resolver,
		membershipUpdateCh:  make(chan *membership.ChangedEvent, 10),
		shardMgr:            shardMgr,
		historyMgr:          historyMgr,
		metadataMgr:         metadataMgr,
		executionMgrFactory: executionMgrFactory,
		engineFactory:       factory,
		historyShards:       make(map[int]*historyShardsItem),
		shardClosedCh:       make(chan int, config.NumberOfShards),
		shutdownCh:          make(chan struct{}),
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueShardController,
		}),
		config:          config,
		metricsClient:   metricsClient,
		clusterMetadata: clusterMetadata,
	}
}

func newHistoryShardsItem(shardID int, shardMgr persistence.ShardManager, historyMgr persistence.HistoryManager,
	metadataMgr persistence.MetadataManager, executionMgrFactory persistence.ExecutionManagerFactory, factory EngineFactory,
	host *membership.HostInfo, config *Config, logger bark.Logger, metricsClient metrics.Client, clusterMetadata cluster.Metadata) (*historyShardsItem, error) {

	executionMgr, err := executionMgrFactory.CreateExecutionManager(shardID)
	if err != nil {
		return nil, err
	}

	domainCache := cache.NewDomainCache(metadataMgr, logger)

	return &historyShardsItem{
		shardID:       shardID,
		shardMgr:      shardMgr,
		historyMgr:    historyMgr,
		executionMgr:  executionMgr,
		domainCache:   domainCache,
		engineFactory: factory,
		host:          host,
		config:        config,
		logger: logger.WithFields(bark.Fields{
			logging.TagHistoryShardID: shardID,
		}),
		metricsClient:   metricsClient,
		clusterMetadata: clusterMetadata,
	}, nil
}

func (c *shardController) Start() {
	if !atomic.CompareAndSwapInt32(&c.isStarted, 0, 1) {
		return
	}

	c.acquireShards()
	c.shutdownWG.Add(1)
	go c.shardManagementPump()

	c.hServiceResolver.AddListener(shardControllerMembershipUpdateListenerName, c.membershipUpdateCh)

	logging.LogShardControllerStartedEvent(c.logger, c.host.Identity())
}

func (c *shardController) Stop() {
	if !atomic.CompareAndSwapInt32(&c.isStopped, 0, 1) {
		return
	}

	c.Lock()
	c.isStopping = true
	c.Unlock()

	if atomic.LoadInt32(&c.isStarted) == 1 {
		if err := c.hServiceResolver.RemoveListener(shardControllerMembershipUpdateListenerName); err != nil {
			logging.LogOperationFailedEvent(c.logger, "Error removing membership update listerner", err)
		}
		close(c.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		logging.LogShardControllerShutdownTimedoutEvent(c.logger, c.host.Identity())
	}

	logging.LogShardControllerShutdownEvent(c.logger, c.host.Identity())
}

func (c *shardController) GetEngine(workflowID string) (Engine, error) {
	shardID := c.config.GetShardID(workflowID)
	return c.getEngineForShard(shardID)
}

func (c *shardController) getEngineForShard(shardID int) (Engine, error) {
	sw := c.metricsClient.StartTimer(metrics.HistoryShardControllerScope, metrics.GetEngineForShardLatency)
	defer sw.Stop()
	item, err := c.getOrCreateHistoryShardItem(shardID)
	if err != nil {
		return nil, err
	}
	return item.getOrCreateEngine(c.shardClosedCh)
}

func (c *shardController) removeEngineForShard(shardID int) {
	sw := c.metricsClient.StartTimer(metrics.HistoryShardControllerScope, metrics.RemoveEngineForShardLatency)
	defer sw.Stop()
	item, _ := c.removeHistoryShardItem(shardID)
	if item != nil {
		item.stopEngine()
	}
}

func (c *shardController) getOrCreateHistoryShardItem(shardID int) (*historyShardsItem, error) {
	c.RLock()
	if item, ok := c.historyShards[shardID]; ok {
		c.RUnlock()
		return item, nil
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	if item, ok := c.historyShards[shardID]; ok {
		return item, nil
	}

	if c.isStopping {
		return nil, fmt.Errorf("shardController for host '%v' shutting down", c.host.Identity())
	}
	info, err := c.hServiceResolver.Lookup(string(shardID))
	if err != nil {
		return nil, err
	}

	if info.Identity() == c.host.Identity() {
		shardItem, err := newHistoryShardsItem(shardID, c.shardMgr, c.historyMgr, c.metadataMgr,
			c.executionMgrFactory, c.engineFactory, c.host, c.config, c.logger, c.metricsClient, c.clusterMetadata)
		if err != nil {
			return nil, err
		}
		c.historyShards[shardID] = shardItem
		c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.ShardItemCreatedCounter)
		logging.LogShardItemCreatedEvent(shardItem.logger, info.Identity(), shardID)
		return shardItem, nil
	}

	return nil, createShardOwnershipLostError(c.host.Identity(), info.GetAddress())
}

func (c *shardController) removeHistoryShardItem(shardID int) (*historyShardsItem, error) {
	nShards := 0
	c.Lock()
	item, ok := c.historyShards[shardID]
	if !ok {
		c.Unlock()
		return nil, fmt.Errorf("No item found to remove for shard: %v", shardID)
	}
	delete(c.historyShards, shardID)
	nShards = len(c.historyShards)
	c.Unlock()

	c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.ShardItemRemovedCounter)
	logging.LogShardItemRemovedEvent(item.logger, c.host.Identity(), shardID, nShards)
	return item, nil
}

// shardManagementPump is the main event loop for
// shardController. It is responsible for acquiring /
// releasing shards in response to any event that can
// change the shard ownership. These events are
//   a. Ring membership change
//   b. Periodic ticker
//   c. ShardOwnershipLostError and subsequent ShardClosedEvents from engine
func (c *shardController) shardManagementPump() {

	defer c.shutdownWG.Done()

	acquireTicker := time.NewTicker(c.config.AcquireShardInterval)
	defer acquireTicker.Stop()

	for {

		select {
		case <-c.shutdownCh:
			c.doShutdown()
			return
		case <-acquireTicker.C:
			c.acquireShards()
		case changedEvent := <-c.membershipUpdateCh:
			c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.MembershipChangedCounter)
			logging.LogRingMembershipChangedEvent(c.logger, c.host.Identity(), len(changedEvent.HostsAdded),
				len(changedEvent.HostsRemoved), len(changedEvent.HostsUpdated))
			c.acquireShards()
		case shardID := <-c.shardClosedCh:
			c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.ShardClosedCounter)
			logging.LogShardClosedEvent(c.logger, c.host.Identity(), shardID)
			c.removeEngineForShard(shardID)
			// The async close notifications can cause a race
			// between acquire/release when nodes are flapping
			// The impact of this race is un-necessary shard load/unloads
			// even though things will settle eventually
			// To reduce the chance of the race happening, lets
			// process all closed events at once before we attempt
			// to acquire new shards again
			c.processShardClosedEvents()
		}
	}
}

func (c *shardController) acquireShards() {

	c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.AcquireShardsCounter)
	sw := c.metricsClient.StartTimer(metrics.HistoryShardControllerScope, metrics.AcquireShardsLatency)
	defer sw.Stop()

AcquireLoop:
	for shardID := 0; shardID < c.config.NumberOfShards; shardID++ {
		info, err := c.hServiceResolver.Lookup(string(shardID))
		if err != nil {
			logging.LogOperationFailedEvent(c.logger, fmt.Sprintf("Error looking up host for shardID: %v", shardID), err)
			continue AcquireLoop
		}

		if info.Identity() == c.host.Identity() {
			_, err1 := c.getEngineForShard(shardID)
			if err1 != nil {
				c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.GetEngineForShardErrorCounter)
				logging.LogOperationFailedEvent(c.logger, fmt.Sprintf("Unable to create history shard engine: %v", shardID),
					err1)
				continue AcquireLoop
			}
		} else {
			c.removeEngineForShard(shardID)
		}
	}

	c.metricsClient.UpdateGauge(metrics.HistoryShardControllerScope, metrics.NumShardsGauge, float64(c.numShards()))
}

func (c *shardController) doShutdown() {
	logging.LogShardControllerShuttingDownEvent(c.logger, c.host.Identity())
	c.Lock()
	defer c.Unlock()
	for _, item := range c.historyShards {
		item.stopEngine()
	}
	c.historyShards = nil
}

func (c *shardController) processShardClosedEvents() {
	for {
		select {
		case shardID := <-c.shardClosedCh:
			c.metricsClient.IncCounter(metrics.HistoryShardControllerScope, metrics.ShardClosedCounter)
			logging.LogShardClosedEvent(c.logger, c.host.Identity(), shardID)
			c.removeEngineForShard(shardID)
		default:
			return
		}
	}
}

func (c *shardController) numShards() int {
	nShards := 0
	c.RLock()
	nShards = len(c.historyShards)
	c.RUnlock()
	return nShards
}

func (i *historyShardsItem) getEngine() Engine {
	i.RLock()
	defer i.RUnlock()

	return i.engine
}

func (i *historyShardsItem) getOrCreateEngine(shardClosedCh chan<- int) (Engine, error) {
	i.RLock()
	if i.engine != nil {
		defer i.RUnlock()
		return i.engine, nil
	}
	i.RUnlock()

	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		return i.engine, nil
	}

	logging.LogShardEngineCreatingEvent(i.logger, i.host.Identity(), i.shardID)

	context, err := acquireShard(i.shardID, i.shardMgr, i.historyMgr, i.executionMgr, i.domainCache, i.host.Identity(), shardClosedCh,
		i.config, i.logger, i.metricsClient, i.clusterMetadata)
	if err != nil {
		return nil, err
	}

	i.engine = i.engineFactory.CreateEngine(context)
	i.engine.Start()

	logging.LogShardEngineCreatedEvent(i.logger, i.host.Identity(), i.shardID)
	return i.engine, nil
}

func (i *historyShardsItem) stopEngine() {
	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		logging.LogShardEngineStoppingEvent(i.logger, i.host.Identity(), i.shardID)
		i.engine.Stop()
		i.engine = nil
		logging.LogShardEngineStoppedEvent(i.logger, i.host.Identity(), i.shardID)
	}
}

func isShardOwnershiptLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}
