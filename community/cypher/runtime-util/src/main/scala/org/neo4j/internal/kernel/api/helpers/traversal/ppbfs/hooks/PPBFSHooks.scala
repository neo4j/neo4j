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

import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost

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

  def setInstance(hooks: PPBFSHooks): Unit = { current = hooks }
}

abstract class PPBFSHooks {
  // NodeState
  def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
  def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {}
  def propagateLengthPair(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit = {}
  def validateLengthState(nodeState: NodeState, lengthFromSource: Int, tracedLengthToTarget: Int): Unit = {}

  // PathTracer
  def returnPath(tracedPath: PathTracer.TracedPath): Unit = {}
  def invalidTrail(getTracedPath: () => PathTracer.TracedPath): Unit = {}
  def skippingDuplicateRelationship(getTracedPath: () => PathTracer.TracedPath): Unit = {}
  def activateSignpost(currentLength: Int, child: TwoWaySignpost): Unit = {}
  def deactivateSignpost(currentLength: Int, last: TwoWaySignpost): Unit = {}

  // PGPathPropagatingBFS
  def nextLevel(currentDepth: Int): Unit = {}
  def noMoreNodes(): Unit = {}

  // DataManager
  def propagateAll(
    nodesToPropagate: HeapTrackingIntObjectHashMap[HeapTrackingIntObjectHashMap[HeapTrackingUnifiedSet[NodeState]]],
    totalLength: Int
  ): Unit = {}
  def propagateAllAtLengths(lengthFromSource: Int, lengthToTarget: Int): Unit = {}
  def schedulePropagation(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit = {}
  def newRow(nodeId: Long): Unit = {}
  def decrementTargetCount(nodeState: NodeState, remainingTargetCount: Int): Unit = {}
  def addTarget(nodeState: NodeState): Unit = {}

  // Signpost
  def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
  def setVerified(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
}
