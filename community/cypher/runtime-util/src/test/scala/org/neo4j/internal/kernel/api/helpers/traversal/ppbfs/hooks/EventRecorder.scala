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

import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddTarget
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Event
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Expand
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ExpandNode
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.PropagateLengthPair
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ReturnPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.SchedulePropagation

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[ppbfs] class EventRecorder {

  private val events = ArrayBuffer.empty[EventRecorder.Event]

  private def record(event: Event): EventRecorder = {
    events += event
    this
  }

  def getEvents: Seq[EventRecorder.Event] = events.toSeq

  def nextLevel(depth: Int): EventRecorder =
    record(NextLevel(depth))

  def propagateLengthPair(nodeId: Long, lengthFromSource: Int, lengthToTarget: Int): EventRecorder =
    record(PropagateLengthPair(nodeId, lengthFromSource, lengthToTarget))

  def schedulePropagation(nodeId: Long, lengthFromSource: Int, lengthToTarget: Int): EventRecorder =
    record(SchedulePropagation(nodeId, lengthFromSource, lengthToTarget))

  def returnPath(entities: Long*): EventRecorder =
    record(ReturnPath(entities))

  def addTarget(id: Long): EventRecorder =
    record(AddTarget(id))

  def expand(direction: TraversalDirection, forwardDepth: Int, backwardDepth: Int): EventRecorder =
    record(Expand(direction, forwardDepth, backwardDepth))

  def expandNode(node: Long, direction: TraversalDirection): EventRecorder =
    record(ExpandNode(node, direction))
}

private[ppbfs] object EventRecorder {
  sealed trait Event
  case class NextLevel(depth: Int) extends Event
  case class PropagateLengthPair(nodeId: Long, lengthFromSource: Int, lengthToTarget: Int) extends Event
  case class SchedulePropagation(nodeId: Long, lengthFromSource: Int, lengthToTarget: Int) extends Event
  case class ReturnPath(entities: Seq[Long]) extends Event
  case class AddTarget(nodeId: Long) extends Event
  case class Expand(direction: TraversalDirection, forwardDepth: Int, backwardDepth: Int) extends Event
  case class ExpandNode(node: Long, direction: TraversalDirection) extends Event

  implicit class RichEventSeq(events: Seq[Event]) {

    def ofType[E: ClassTag]: Seq[E] =
      events.collect {
        case e: E => e
      }
  }
}
