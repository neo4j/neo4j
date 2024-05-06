/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.FoundNodes
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GlobalState.ScheduleSource
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.Propagator.NodeStateSkipList
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

/**
 * Provides a way to inspect the progress of the algorithm for the purposes of logging and testing.
 * <p>
 * The production environment should use [[PPBFSHooks.NULL]]
 */
object PPBFSHooks {
  val NULL: PPBFSHooks = new PPBFSHooks {}

  private var current: PPBFSHooks = null

  def getInstance(): PPBFSHooks = {
    if (current == null) current = NULL
    current
  }

  def setInstance(hooks: PPBFSHooks): Unit = {
    current = hooks
  }
}

abstract class PPBFSHooks {
  def newRow(nodeId: Long): Unit = {}

  // NodeState
  def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
  def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {}
  def propagateLengthPair(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit = {}
  def validateSourceLength(nodeState: NodeState, lengthFromSource: Int, tracedLengthToTarget: Int): Unit = {}

  // PathTracer
  def returnPath(tracedPath: PathTracer.TracedPath): Unit = {}
  def invalidTrail(getTracedPath: () => PathTracer.TracedPath): Unit = {}
  def skippingDuplicateRelationship(getTracedPath: () => PathTracer.TracedPath): Unit = {}
  def activateSignpost(currentLength: Int, child: TwoWaySignpost): Unit = {}
  def deactivateSignpost(currentLength: Int, last: TwoWaySignpost): Unit = {}

  // PGPathPropagatingBFS
  def nextLevel(currentDepth: Int): Unit = {}
  def trace(currentDepth: Int): Unit = {}
  def noMoreNodes(): Unit = {}

  // Propagator
  def propagate(
    nodesToPropagate: HeapTrackingIntObjectHashMap[HeapTrackingIntObjectHashMap[NodeStateSkipList]],
    totalLength: Int
  ): Unit = {}
  def propagateAllAtLengths(lengthFromSource: Int, lengthToTarget: Int): Unit = {}
  def schedule(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int, source: ScheduleSource): Unit = {}

  // TargetTracker
  def decrementTargetCount(nodeState: NodeState, remainingTargetCount: Int): Unit = {}
  def addTarget(nodeState: NodeState): Unit = {}

  // Signpost
  def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
  def setVerified(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
  def addSourceLength(signpost: TwoWaySignpost, sourceLength: Int): Unit = {}

  // BFSExpander
  def expand(direction: TraversalDirection, foundNodes: FoundNodes): Unit = {}
  def expandNode(nodeId: Long, states: HeapTrackingArrayList[State], direction: TraversalDirection): Unit = {}
  def discover(node: NodeState, direction: TraversalDirection): Unit = {}
}
