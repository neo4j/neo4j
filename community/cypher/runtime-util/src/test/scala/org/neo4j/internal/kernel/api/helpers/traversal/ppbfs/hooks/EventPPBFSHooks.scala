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
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeData
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost

private[ppbfs] class EventPPBFSHooks(recorder: EventRecorder) extends PPBFSHooks {

  def this() {
    this(new EventRecorder())
  }

  // NodeData
  override def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    signpost match {
      case n: TwoWaySignpost.NodeSignpost =>
        recorder.addNodeSourceSignpost(n.prevNode.id(), lengthFromSource)
      case r: TwoWaySignpost.RelSignpost =>
        recorder.addRelSourceSignpost(r.prevNode.id(), r.relId, r.forwardNode.id(), lengthFromSource)
    }
  }

  override def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {
    signpost match {
      case n: TwoWaySignpost.NodeSignpost =>
        recorder.addNodeTargetSignpost(n.prevNode.id(), lengthToTarget)
      case r: TwoWaySignpost.RelSignpost =>
        recorder.addRelTargetSignpost(r.prevNode.id(), r.relId, r.forwardNode.id(), lengthToTarget)
    }
  }

  override def propagateLengthPair(nodeData: NodeData, lengthFromSource: Int, lengthToTarget: Int): Unit = {}

  override def validateLengthState(nodeData: NodeData, lengthFromSource: Int, tracedLengthToTarget: Int): Unit = {}

  // PathTracer
  override def returnPath(tracedPath: PathTracer.TracedPath): Unit = {}

  override def invalidTrail(getTracedPath: () => PathTracer.TracedPath): Unit = {}

  override def skippingDuplicateRelationship(
    target: NodeData,
    activePath: HeapTrackingArrayList[TwoWaySignpost]
  ): Unit = {}

  override def activateSignpost(currentLength: Int, child: TwoWaySignpost): Unit = {}

  override def deactivateSignpost(currentLength: Int, last: TwoWaySignpost): Unit = {}

  // PGPathPropagatingBFS
  override def nextLevel(depth: Int): Unit = {
    recorder.nextLevel(depth)
  }

  override def noMoreNodes(): Unit = {
    recorder.noMoreNodes()
  }

  // DataManager
  override def propagateAll(
    nodesToPropagate: HeapTrackingIntObjectHashMap[HeapTrackingIntObjectHashMap[HeapTrackingUnifiedSet[NodeData]]],
    totalLength: Int
  ): Unit = {}

  override def propagateAllAtLengths(lengthFromSource: Int, lengthToTarget: Int): Unit = {}

  override def registerNodeToPropagate(nodeData: NodeData, lengthFromSource: Int, lengthToTarget: Int): Unit = {}

  override def newRow(nodeId: Long): Unit = {
    recorder.newSource(nodeId)
  }

  override def finishedPropagation(targets: HeapTrackingArrayList[NodeData]): Unit = {}

  override def decrementTargetCount(nodeData: NodeData, remainingTargetCount: Int): Unit = {
    recorder.decrementTargetCount(nodeData.id(), remainingTargetCount)
  }

  // Signpost
  override def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}

  override def setVerified(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {}
}
