package org.neo4j.cypher.internal.config

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingChangeListener
import org.neo4j.memory.MemoryTracker

/**
 * Logical description of memory tracking behaviour
 */
sealed trait MemoryTracking
case object NO_TRACKING extends MemoryTracking
case object MEMORY_TRACKING extends MemoryTracking
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

  config.addListener(GraphDatabaseSettings.track_query_allocation,
    new SettingChangeListener[java.lang.Boolean] {
      override def accept(before: java.lang.Boolean, after: java.lang.Boolean): Unit =
        _memoryTracking = getMemoryTracking(after)
    })

  private def getMemoryTracking(trackQueryAllocation: Boolean): MemoryTracking =
    if (trackQueryAllocation) MEMORY_TRACKING
    else NO_TRACKING
}

case class CUSTOM_MEMORY_TRACKING_CONTROLLER(decorator: MemoryTracker => MemoryTracker) extends MemoryTrackingController {
  override def memoryTracking(doProfile: Boolean): MemoryTracking = CUSTOM_MEMORY_TRACKING(decorator)
}
