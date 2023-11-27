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

import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddNodeSourceSignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddNodeTargetSignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddRelSourceSignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddRelTargetSignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.DecrementTargetCount
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Event
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NewSource
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NoMoreNodes

import scala.collection.mutable.ArrayBuffer

private[ppbfs] class EventRecorder {

  private val events = ArrayBuffer.empty[EventRecorder.Event]

  private def record(event: Event): EventRecorder = {
    events += event
    this
  }

  def newSource(source: Long): EventRecorder =
    record(NewSource(source))

  def noMoreNodes(): EventRecorder =
    record(NoMoreNodes)

  def nextLevel(depth: Int): EventRecorder =
    record(NextLevel(depth))

  def decrementTargetCount(nodeId: Long, remainingCount: Int): EventRecorder =
    record(DecrementTargetCount(nodeId, remainingCount))

  def addNodeSourceSignpost(nodeId: Long, lengthFromSource: Int): EventRecorder =
    record(AddNodeSourceSignpost(nodeId, lengthFromSource))

  def addRelSourceSignpost(fromId: Long, relId: Long, toId: Long, lengthFromSource: Int): EventRecorder =
    record(AddRelSourceSignpost(fromId, relId, toId, lengthFromSource))

  def addNodeTargetSignpost(nodeId: Long, lengthFromSource: Int): EventRecorder =
    record(AddNodeTargetSignpost(nodeId, lengthFromSource))

  def addRelTargetSignpost(fromId: Long, relId: Long, toId: Long, lengthFromSource: Int): EventRecorder =
    record(AddRelTargetSignpost(fromId, relId, toId, lengthFromSource))
}

private[ppbfs] object EventRecorder {
  sealed trait Event
  case class NewSource(source: Long) extends Event
  case object NoMoreNodes extends Event
  case class NextLevel(depth: Int) extends Event
  case class DecrementTargetCount(nodeId: Long, remainingCount: Int) extends Event
  case class AddNodeSourceSignpost(nodeId: Long, lengthFromSource: Int) extends Event
  case class AddRelSourceSignpost(fromId: Long, relId: Long, toId: Long, lengthFromSource: Int) extends Event
  case class AddNodeTargetSignpost(nodeId: Long, lengthFromSource: Int) extends Event
  case class AddRelTargetSignpost(fromId: Long, relId: Long, toId: Long, lengthFromSource: Int) extends Event
}
