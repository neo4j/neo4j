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
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.FoundNodes
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GlobalState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.SignpostStack
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TracedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

private[ppbfs] class EventPPBFSHooks(recorder: EventRecorder) extends PPBFSHooks {

  override def propagateLengthPair(nodeData: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit = {
    recorder.propagateLengthPair(nodeData.id(), lengthFromSource, lengthToTarget)
  }

  override def returnPath(signposts: SignpostStack): Unit = {
    recorder.returnPath(TracedPath.fromSignpostStack(signposts).entities.map(_.id): _*)
  }

  override def nextLevel(depth: Int): Unit = {
    recorder.nextLevel(depth)
  }

  override def schedule(
    nodeData: NodeState,
    lengthFromSource: Int,
    lengthToTarget: Int,
    source: GlobalState.ScheduleSource
  ): Unit = {
    recorder.schedulePropagation(nodeData.id(), lengthFromSource, lengthToTarget)
  }

  override def addTarget(nodeData: NodeState): Unit = {
    recorder.addTarget(nodeData.id())
  }

  override def expand(direction: TraversalDirection, foundNodes: FoundNodes): Unit = {
    recorder.expand(direction, foundNodes.forwardDepth(), foundNodes.backwardDepth())
  }

  override def expandNode(nodeId: Long, states: HeapTrackingArrayList[State], direction: TraversalDirection): Unit = {
    recorder.expandNode(nodeId, direction)
  }
}
