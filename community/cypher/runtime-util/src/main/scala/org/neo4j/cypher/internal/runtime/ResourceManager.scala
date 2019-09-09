/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import java.util

import org.neo4j.cypher.internal.runtime.ResourceManager.INITIAL_CAPACITY
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.internal.kernel.api.{AutoCloseablePlus, CloseListener}

import scala.collection.JavaConverters._

class ResourceManager(monitor: ResourceMonitor = ResourceMonitor.NOOP) extends CloseableResource with CloseListener {
  private val resources: util.Collection[AutoCloseablePlus] = new util.ArrayList[AutoCloseablePlus](INITIAL_CAPACITY)

  /**
   * Trace a resource
   */
  def trace(resource: AutoCloseablePlus): Unit = {
    monitor.trace(resource)
    resources.add(resource)
    resource.setCloseListener(this)
  }

  /**
   * Called when the resource is closed.
   */
  override def onClosed(resource: AutoCloseablePlus): Unit = {
    monitor.close(resource)
    // close is idempotent and can be called multiple times, but we want to get here only once.
    resource.setCloseListener(null)
    if (!resources.remove(resource)) {
      throw new IllegalStateException(s"$resource is not in the resource set $resources")
    }
  }

  def allResources: Iterable[AutoCloseablePlus] = resources.asScala

  override def close(): Unit = {
    val iterator = resources.iterator()
    var error: Throwable = null
    while (iterator.hasNext) {
      try {
        val resource = iterator.next()
        monitor.close(resource)
        resource.setCloseListener(null) // We don't want a call to onClosed any longer
        resource.close()
      }
      catch {
        case t: Throwable => error = Exceptions.chain(error, t)
      }
    }
    if (error != null) throw error
    else {
      resources.clear()
    }
  }
}

object ResourceManager {
  val INITIAL_CAPACITY: Int = 8
}

trait ResourceMonitor {
  def trace(resource: AutoCloseablePlus): Unit
  def close(resource: AutoCloseablePlus): Unit
}

object ResourceMonitor {
  val NOOP: ResourceMonitor = new ResourceMonitor {
    def trace(resource: AutoCloseablePlus): Unit = {}
    def close(resource: AutoCloseablePlus): Unit = {}
  }
}
