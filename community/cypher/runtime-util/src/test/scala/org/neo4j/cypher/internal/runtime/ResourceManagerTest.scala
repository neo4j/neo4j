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

import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.{AutoCloseablePlus, CloseListener}

import scala.util.Try

class ResourceManagerTest extends CypherFunSuite {

  test("should be able to trace and release a resource") {
    val resource = mock[AutoCloseablePlus]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.onClosed(resource)
    verifyMonitorClose(resource, monitor)

    resources.close()
    verify(resource).getToken
    verifyNoMoreInteractions(resource, monitor)
  }

  test("should close the unreleased resource when closed") {
    val resource = mock[AutoCloseablePlus]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.close()
    verifyClose(resource, monitor)
    verifyNoMoreInteractions(resource, monitor)
  }

  test("should not close resources multiple times when closed") {
    val resource = mock[AutoCloseablePlus]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource, monitor, resources)

    resources.close()
    verifyClose(resource, monitor)
    resources.close()
    verifyNoMoreInteractions(resource, monitor)
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
    val resource1 = mock[AutoCloseablePlus]
    val resource2 = mock[AutoCloseablePlus]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)

    resources.close()
    verifyClose(resource1, monitor)
    verifyClose(resource2, monitor)
    verifyNoMoreInteractions(resource1, resource2, monitor)
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
    val resource1 = mock[AutoCloseablePlus]
    val resource2 = mock[AutoCloseablePlus]
    val resource3 = mock[AutoCloseablePlus]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    verifyTrace(resource1, monitor, resources)
    verifyTrace(resource2, monitor, resources)
    verifyTrace(resource3, monitor, resources)

    val exception1 = new RuntimeException
    when(resource1.close()).thenThrow(exception1)
    val exception2 = new RuntimeException
    when(resource2.close()).thenThrow(exception2)

    val throwable = Try(resources.close()).failed.get
    verifyClose(resource1, monitor)
    verifyClose(resource2, monitor)
    verifyClose(resource3, monitor)
    verifyNoMoreInteractions(resource1, resource2, resource3, monitor)
    Set(throwable) ++ throwable.getSuppressed shouldBe Set(exception1, exception2)
  }

  test("Resource pool should be able to grow beyond initial capacity") {
    //given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor])

    //when
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])

    //then
    pool.all().size shouldBe 5
  }

  test("Should be able to remove resource") {
    val resources = Array(new DummyResource,
                          new DummyResource,
                          new DummyResource,
                          new DummyResource,
                          new DummyResource)
    for (i <- resources.indices) {
      //given
      val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor])
      resources.foreach(pool.add)
      val toRemove = resources(i)

      //when
      pool.remove(toRemove)

      //then
      val closeables = pool.all().toList
      closeables.size shouldBe 4
      closeables shouldNot contain(toRemove)
    }
  }

  test("Should be able to clear") {
    //given
    val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])
    pool.add(mock[AutoCloseablePlus])

    //when
    pool.clear()

    //then
    pool.all() shouldBe empty
  }

  test("Should not call close on removed item") {
    for(i <- 0 until 5) {
      //given
      val resources = Array(new DummyResource,
                            new DummyResource,
                            new DummyResource,
                            new DummyResource,
                            new DummyResource)
      val pool = new SingleThreadedResourcePool(4, mock[ResourceMonitor])
      resources.foreach(pool.add)
      pool.remove(resources(i))

      //when
      pool.closeAll()

      //then
      for(j <- 0 until 5) {
        if (i == j) {
          resources(j).isClosed shouldBe false
        } else {
          resources(j).isClosed shouldBe true
        }
      }
    }
  }

  private def verifyTrace(resource: AutoCloseablePlus, monitor: ResourceMonitor, resources: ResourceManager): Unit = {
    resources.trace(resource)
    verify(resource).setCloseListener(resources)
    verify(resource).setToken(anyInt())
    verify(monitor).trace(resource)
  }

  private def verifyTrace(resource: DummyResource, monitor: ResourceMonitor, resources: ResourceManager): Unit = {
    resources.trace(resource)
    resource.getCloseListener should equal(resources)
    resource.getToken should be >= 0
    verify(monitor).trace(resource)
  }

  private def verifyMonitorClose(resource: AutoCloseablePlus, monitor: ResourceMonitor): Unit = {
    verify(monitor).close(resource)
    verify(resource).setCloseListener(null)
  }

  private def verifyMonitorClose(resource: DummyResource, monitor: ResourceMonitor): Unit = {
    verify(monitor).close(resource)
    resource.getCloseListener shouldBe null
  }

  private def verifyClose(resource: AutoCloseablePlus, monitor: ResourceMonitor): Unit = {
    verifyMonitorClose(resource, monitor)
    verify(resource).setCloseListener(null)
    verify(resource).close()
  }

  private def verifyClose(resource: DummyResource, monitor: ResourceMonitor): Unit = {
    verifyMonitorClose(resource, monitor)
    resource.getCloseListener shouldBe null
    resource.isClosed shouldBe true
  }

  class DummyResource extends AutoCloseablePlus {
    private var listener: CloseListener = _
    private var token = -1
    private var closed = false

    override def close(): Unit = {
      this.closed = true
    }
    override def closeInternal(): Unit = {
      close()
    }
    override def isClosed: Boolean = closed
    override def setCloseListener(closeListener: CloseListener): Unit = {
      this.listener = closeListener
    }
    override def getCloseListener: CloseListener = listener
    override def setToken(token: Int): Unit = {
      this.token = token
    }
    override def getToken: Int = token
  }

}
