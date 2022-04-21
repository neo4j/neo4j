/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.config

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingChangeListener
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
  def memoryTracking(doProfile: Boolean): MemoryTracking
}

class ConfigMemoryTrackingController(config: Config) extends MemoryTrackingController {

  @volatile private var _memoryTracking: MemoryTracking =
    getMemoryTracking(config.get(GraphDatabaseSettings.track_query_allocation))

  override def memoryTracking(doProfile: Boolean): MemoryTracking =
    if (doProfile && _memoryTracking == NO_TRACKING) {
      getMemoryTracking(trackQueryAllocation = true)
    } else {
      _memoryTracking
    }

  config.addListener(
    GraphDatabaseSettings.track_query_allocation,
    new SettingChangeListener[java.lang.Boolean] {

      override def accept(before: java.lang.Boolean, after: java.lang.Boolean): Unit =
        _memoryTracking = getMemoryTracking(after)
    }
  )

  private def getMemoryTracking(trackQueryAllocation: Boolean): MemoryTracking =
    if (trackQueryAllocation) MEMORY_TRACKING
    else NO_TRACKING
}

object CUSTOM_MEMORY_TRACKING_CONTROLLER {
  type MemoryTrackerDecorator = MemoryTracker => MemoryTracker
}

case class CUSTOM_MEMORY_TRACKING_CONTROLLER(decorator: MemoryTrackerDecorator) extends MemoryTrackingController {
  override def memoryTracking(doProfile: Boolean): MemoryTracking = CUSTOM_MEMORY_TRACKING(decorator)
}
