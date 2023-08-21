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
package org.neo4j.cypher.internal.runtime

import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.runtime.DummyResource.verifyClose
import org.neo4j.cypher.internal.runtime.DummyResource.verifyMonitorClose
import org.neo4j.cypher.internal.runtime.DummyResource.verifyTrace
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.AutoCloseablePlus.UNTRACKED
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker

import scala.util.Try

class ResourceManagerTest extends CypherFunSuite {

  test("should be able to trace and release a resource") {
    val resource = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.onClosed(resource)
    verifyMonitorClose(resource, monitor)

    resources.close()
    resource.getToken shouldBe UNTRACKED
    verifyNoMoreInteractions(monitor)
  }

  test("should close the unreleased resource when closed") {
    val resource = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.close()
    verifyClose(resource, monitor)
    verifyNoMoreInteractions(monitor)
  }

  test("should not close resources multiple times when closed") {
    val resource = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.close()
    verifyClose(resource, monitor)
    resources.close()
    verifyNoMoreInteractions(monitor)
  }

  test("should be able to trace and release multiple resources") {
    val resource1 = new DummyResource
    val resource2 = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)

    resources.onClosed(resource1)
    verifyMonitorClose(resource1, monitor)
    resources.onClosed(resource2)
    verifyMonitorClose(resource2, monitor)

    resources.close()
    verifyNoMoreInteractions(monitor)
  }

  test("should close the unreleased resources when closed") {
    val resource1 = new DummyResource
    val resource2 = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)

    resources.close()
    verifyClose(resource1, monitor)
    verifyClose(resource2, monitor)
    verifyNoMoreInteractions(monitor)
  }

  test("should close only the unreleased resources when closed") {
    val resource1 = new DummyResource
    val resource2 = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)

    resources.onClosed(resource2)
    verifyMonitorClose(resource2, monitor)

    resources.close()
    verifyClose(resource1, monitor)
    verifyNoMoreInteractions(monitor)
  }

  test("should close all the resources even in case of exceptions") {
    val exception1 = new RuntimeException
    val exception2 = new RuntimeException
    val resource1 = new ThrowingDummyResource(exception1)
    val resource2 = new ThrowingDummyResource(exception2)
    val resource3 = new DummyResource
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)
    verifyTrace(resource3, monitor, resources)
    val throwable = Try(resources.close()).failed.get
    verifyClose(resource1, monitor)
    verifyClose(resource2, monitor)
    verifyClose(resource3, monitor)
    verifyNoMoreInteractions(monitor)
    Set(throwable) ++ throwable.getSuppressed shouldBe Set(exception1, exception2)
  }

  test("Resource pool should be able to grow beyond initial capacity") {
    // given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], EmptyMemoryTracker.INSTANCE)

    // when
    pool.add(new DummyResource)
    pool.add(new DummyResource)
    pool.add(new DummyResource)
    pool.add(new DummyResource)
    pool.add(new DummyResource)

    // then
    pool.all().size shouldBe 5
  }

  test("Resource pool shouldn't grow if we add the same resource multiple times") {
    // given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], EmptyMemoryTracker.INSTANCE)

    // when
    val resource = new DummyResource
    pool.add(resource)
    pool.add(resource)
    pool.add(resource)
    pool.add(resource)
    pool.add(resource)
    pool.add(resource)

    // then
    pool.all().size shouldBe 1
    resource.getToken shouldBe 0
  }

  test("Should handle removing the same item multiple times") {
    // given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], EmptyMemoryTracker.INSTANCE)

    // when
    val resource = new DummyResource
    pool.add(resource)

    pool.remove(resource)
    pool.remove(resource)
    pool.remove(resource)
    pool.remove(resource)
    pool.remove(resource)

    // then
    pool.all().size shouldBe 0
    resource.getToken shouldBe UNTRACKED
  }

  test("Should be able to remove resource") {
    val resources = Array(new DummyResource, new DummyResource, new DummyResource, new DummyResource, new DummyResource)
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], EmptyMemoryTracker.INSTANCE)
    for (i <- resources.indices) {
      // given
      resources.foreach(pool.add)
      val toRemove = resources(i)

      // when
      pool.remove(toRemove)

      // then
      val closeables = pool.all().toList
      closeables.size shouldBe 4
      closeables shouldNot contain(toRemove)
    }
  }

  test("Should not call close on removed item") {
    for (i <- 0 until 5) {
      // given
      val resources =
        Array(new DummyResource, new DummyResource, new DummyResource, new DummyResource, new DummyResource)
      val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], EmptyMemoryTracker.INSTANCE)
      resources.foreach(pool.add)
      pool.remove(resources(i))

      // when
      pool.closeAll()

      // then
      for (j <- 0 until 5) {
        if (i == j) {
          resources(j).isClosed shouldBe false
        } else {
          resources(j).isClosed shouldBe true
        }
      }
    }
  }

  test("Resource pool new size should not overflow") {
    // given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], mock[MemoryTracker])

    pool.computeNewSize(1) shouldBe 2
    pool.computeNewSize(2) shouldBe 4
    pool.computeNewSize(5) shouldBe 10
    pool.computeNewSize(Int.MaxValue / 2) shouldBe Int.MaxValue - 1
    pool.computeNewSize(Int.MaxValue / 2 + 1) shouldBe Int.MaxValue / 2 + 2
    pool.computeNewSize(Int.MaxValue - 1) shouldBe Int.MaxValue
  }

  test("Resource pool attempt not to create holes") {
    // given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor], mock[MemoryTracker])

    val r1 = new DummyResource
    val r2 = new DummyResource
    val r3 = new DummyResource
    val r4 = new DummyResource
    pool.add(r1)
    pool.add(r2)
    pool.add(r3)
    pool.add(r4)

    pool.remove(r3)
    pool.remove(r4)
    // adding resources back shouldn't force the underlying array to grow
    pool.add(r4)
    pool.add(r3)

    pool.allIncludingNullValues should equal(List(r1, r2, r4, r3))
  }

}

class DummyResource extends DefaultCloseListenable {
  private var closed = false

  override def closeInternal(): Unit = {
    this.closed = true
  }

  override def isClosed: Boolean = closed
}

object DummyResource extends CypherFunSuite {

  def verifyTrace(resource: DummyResource, monitor: ResourceMonitor, resources: ResourceManager): Unit = {
    resources.trace(resource)
    resource.getCloseListener should equal(resources)
    resource.getToken should be >= 0
    verify(monitor).trace(resource)
  }

  def verifyMonitorClose(resource: DummyResource, monitor: ResourceMonitor): Unit = {
    verify(monitor).close(resource)
    resource.getCloseListener shouldBe null
  }

  def verifyClose(resource: DummyResource, monitor: ResourceMonitor): Unit = {
    verifyMonitorClose(resource, monitor)
    verifyClose(resource)
  }

  def verifyClose(resource: DummyResource): Unit = {
    resource.getCloseListener shouldBe null
    resource.isClosed shouldBe true
    resource.getToken shouldBe UNTRACKED
  }
}

class ThrowingDummyResource(error: Exception) extends DummyResource {

  override def closeInternal(): Unit = {
    super.closeInternal()
    throw error
  }
}
