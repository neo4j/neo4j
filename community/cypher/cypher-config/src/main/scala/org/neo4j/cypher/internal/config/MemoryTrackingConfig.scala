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
package org.neo4j.cypher.internal.config

import org.neo4j.cypher.internal.config.CUSTOM_MEMORY_TRACKING_CONTROLLER.MemoryTrackerDecorator
import org.neo4j.memory.MemoryTracker

/**
 * Logical description of memory tracking behaviour
 */
sealed trait MemoryTracking
case object NO_TRACKING extends MemoryTracking { def instance: MemoryTracking = this }
case object MEMORY_TRACKING extends MemoryTracking { def instance: MemoryTracking = this }
case class CUSTOM_MEMORY_TRACKING(decorator: MemoryTracker => MemoryTracker) extends MemoryTracking

/**
 * Controller of memory tracking. Needed to make memory tracking dynamically configurable.
 */
trait MemoryTrackingController {
  def memoryTracking: MemoryTracking
}

object CUSTOM_MEMORY_TRACKING_CONTROLLER {
  type MemoryTrackerDecorator = MemoryTracker => MemoryTracker
}

case object MEMORY_TRACKING_ENABLED_CONTROLLER extends MemoryTrackingController {
  override def memoryTracking: MemoryTracking = MEMORY_TRACKING
}

case object MEMORY_TRACKING_DISABLED_CONTROLLER extends MemoryTrackingController {
  override def memoryTracking: MemoryTracking = NO_TRACKING
}

case class CUSTOM_MEMORY_TRACKING_CONTROLLER(decorator: MemoryTrackerDecorator) extends MemoryTrackingController {
  override def memoryTracking: MemoryTracking = CUSTOM_MEMORY_TRACKING(decorator)
}
