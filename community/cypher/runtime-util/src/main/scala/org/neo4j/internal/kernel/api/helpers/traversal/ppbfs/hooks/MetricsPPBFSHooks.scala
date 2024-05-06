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
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GlobalState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

/**
 * Gathers statistics about the internal operations of the algorithm and prints to stdout on closing
 */
class MetricsPPBFSHooks extends PPBFSHooks with AutoCloseable {
  private var sourceSignpostSchedules: Int = 0
  private var propagationSchedules: Int = 0
  private var targetSignpostSchedules: Int = 0
  private var validationSchedules: Int = 0
  private var schedules: Int = 0
  private var expansions: Int = 0
  private var propagations: Int = 0
  private var prunes: Int = 0
  private var skips: Int = 0

  override def propagateLengthPair(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit = {
    propagations += 1
  }

  override def schedule(
    nodeState: NodeState,
    lengthFromSource: Int,
    lengthToTarget: Int,
    source: GlobalState.ScheduleSource
  ): Unit = {
    source match {
      case GlobalState.ScheduleSource.SourceSignpost => sourceSignpostSchedules += 1
      case GlobalState.ScheduleSource.Propagated     => propagationSchedules += 1
      case GlobalState.ScheduleSource.TargetSignpost => targetSignpostSchedules += 1
      case GlobalState.ScheduleSource.Validation     => validationSchedules += 1
    }
    schedules += 1
  }

  override def expandNode(nodeId: Long, states: HeapTrackingArrayList[State], direction: TraversalDirection): Unit = {
    expansions += 1
  }

  override def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    prunes += 1
  }

  override def skippingDuplicateRelationship(getTracedPath: () => PathTracer.TracedPath): Unit = {
    skips += 1
  }

  override def toString: String =
    s"""
       |schedules: $schedules
       |  sourceSignpost: $sourceSignpostSchedules
       |  propagation: $propagationSchedules
       |  targetSignpost: $targetSignpostSchedules
       |  validation: $validationSchedules
       |expansions: $expansions
       |propagations: $propagations
       |prunes:$prunes
       |skips:$skips""".stripMargin

  def close(): Unit = println(toString)
}
