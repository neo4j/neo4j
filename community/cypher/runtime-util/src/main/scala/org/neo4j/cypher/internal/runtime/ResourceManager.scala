/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.ResourceManager.INITIAL_CAPACITY
import org.neo4j.internal.helpers.Exceptions
import org.neo4j.internal.kernel.api.{AutoCloseablePlus, CloseListener}
import org.neo4j.util.Preconditions

class ResourceManager(monitor: ResourceMonitor = ResourceMonitor.NOOP) extends CloseableResource with CloseListener {
  protected val resources: ResourcePool = new SingleThreadedResourcePool(INITIAL_CAPACITY, monitor)

  /**
   * Trace a resource
   */
  def trace(resource: AutoCloseablePlus): Unit = {
    monitor.trace(resource)
    resources.add(resource)
    resource.setCloseListener(this)
  }

  /**
   * Stop tracing a resource, don't close it.
   */
  def untrace(resource: AutoCloseablePlus): Unit = {
    monitor.untrace(resource)
    resources.remove(resource)
    resource.setCloseListener(null)
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

  def allResources: Iterator[AutoCloseablePlus] = resources.all()

  override def close(): Unit = resources.closeAll()
}

class ThreadSafeResourceManager(monitor: ResourceMonitor) extends ResourceManager(monitor) {
  override protected val resources:ResourcePool = new ThreadSafeResourcePool(monitor)
}

object ResourceManager {
  val INITIAL_CAPACITY: Int = 8
}

trait ResourceMonitor {
  def trace(resource: AutoCloseablePlus): Unit
  def untrace(resource: AutoCloseablePlus): Unit
  def close(resource: AutoCloseablePlus): Unit
}

object ResourceMonitor {
  val NOOP: ResourceMonitor = new ResourceMonitor {
    def trace(resource: AutoCloseablePlus): Unit = {}
    def untrace(resource: AutoCloseablePlus): Unit = {}
    def close(resource: AutoCloseablePlus): Unit = {}
  }
}

trait ResourcePool {
  def add(resource: AutoCloseablePlus): Unit
  def remove(resource: AutoCloseablePlus): Boolean
  def all(): Iterator[AutoCloseablePlus]
  def clear(): Unit
  def closeAll(): Unit
}

/**
  * Similar to an ArrayList[AutoCloseablePlus] but does faster removes since it simply set the element to null and
  * does not reorder the backing array.
  * @param capacity the intial capacity of the pool
  * @param monitor the monitor to call on close
  */
class SingleThreadedResourcePool(capacity: Int, monitor: ResourceMonitor) extends ResourcePool {
  private var highMark: Int = 0
  private var closeables: Array[AutoCloseablePlus] = new Array[AutoCloseablePlus](capacity)

  def add(resource: AutoCloseablePlus): Unit = {
    ensureCapacity()
    closeables(highMark) = resource
    resource.setToken(highMark)
    highMark += 1
  }

  def remove(resource: AutoCloseablePlus): Boolean = {
    val i = resource.getToken
    if (i < highMark) {
      if (!(closeables(i) eq resource)) {
        throw new IllegalStateException(s"$resource does not match ${closeables(i)}")
      }
      closeables(i) = null
      if (i == highMark - 1) { //we removed the last item, hence no holes
        highMark -= 1
      }
      true
    } else {
      false
    }
  }

  def all(): Iterator[AutoCloseablePlus] = new Iterator[AutoCloseablePlus] {
    private var offset = 0

    override def hasNext: Boolean = {
      while (offset < highMark && closeables(offset) == null) {
        offset += 1
      }

      offset < highMark
    }

    override def next(): AutoCloseablePlus = {
      if (!hasNext) {
        throw new IndexOutOfBoundsException
      }
      val closeable = closeables(offset)
      offset += 1
      closeable
    }
  }

  def clear(): Unit = {
    highMark = 0
  }

  def closeAll(): Unit = {
    var error: Throwable = null
    var i = 0
    while (i < highMark) {
      try {
        val resource = closeables(i)
        if (resource != null) {
          monitor.close(resource)
          resource.setCloseListener(null) // We don't want a call to onClosed any longer
          resource.close()
        }
      }
      catch {
        case t: Throwable => error = Exceptions.chain(error, t)
      }
      i += 1
    }
    if (error != null) throw error
    else {
      clear()
    }
  }
  private def ensureCapacity(): Unit = {
    if (closeables.length <= highMark) {
      val temp = closeables
      closeables = new Array[AutoCloseablePlus](closeables.length * 2)
      System.arraycopy(temp, 0, closeables, 0, temp.length)
    }
  }
}

class ThreadSafeResourcePool(monitor: ResourceMonitor) extends ResourcePool {
  import scala.collection.JavaConverters._

  val resources: java.util.Collection[AutoCloseablePlus] = new java.util.concurrent.ConcurrentLinkedQueue[AutoCloseablePlus]()

  override def add(resource: AutoCloseablePlus): Unit =
    resources.add(resource)

  override def remove(resource: AutoCloseablePlus): Boolean = resources.remove(resource)

  override def all(): Iterator[AutoCloseablePlus] = resources.iterator().asScala

  override def clear(): Unit = resources.clear()

  override def closeAll(): Unit = {
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
