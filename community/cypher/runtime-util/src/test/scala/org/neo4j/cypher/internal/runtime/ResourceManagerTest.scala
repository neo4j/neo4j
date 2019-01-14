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

import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

import scala.util.Try

class ResourceManagerTest extends CypherFunSuite {

  test("should be able to trace and release a resource") {
    val resource = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource, monitor, resources)

    resources.release(resource)
    close(resource, monitor)

    resources.close(success = true)
    verifyNoMoreInteractions(resource, monitor)
  }

  test("should close the unreleased resource when closed") {
    val resource = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource, monitor, resources)

    resources.close(success = true)
    close(resource, monitor)
    verifyNoMoreInteractions(resource, monitor)
  }

  test("should not close resources multiple times when closed") {
    val resource = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource, monitor, resources)

    resources.close(success = true)
    close(resource, monitor)
    resources.close(success = true)
    verifyNoMoreInteractions(resource, monitor)
  }

  test("should be able to trace and release multiple resources") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource1, monitor, resources)
    trace(resource2, monitor, resources)

    resources.release(resource1)
    close(resource1, monitor)
    resources.release(resource2)
    close(resource2, monitor)

    resources.close(success = true)
    verifyNoMoreInteractions(resource1, resource2, monitor)
  }

  test("should close the unreleased resources when closed") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource1, monitor, resources)
    trace(resource2, monitor, resources)

    resources.close(success = true)
    close(resource1, monitor)
    close(resource2, monitor)
    verifyNoMoreInteractions(resource1, resource2, monitor)
  }

  test("should close only the unreleased resources when closed") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource1, monitor, resources)
    trace(resource2, monitor, resources)

    resources.release(resource2)
    close(resource2, monitor)

    resources.close(success = true)
    close(resource1, monitor)
    verifyNoMoreInteractions(resource1, resource2, monitor)
  }

  test("should close all the resources even in case of exceptions") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resource3 = mock[AutoCloseable]
    val monitor = mock[ResourceMonitor]
    val resources = new ResourceManager(monitor)
    trace(resource1, monitor, resources)
    trace(resource2, monitor, resources)
    trace(resource3, monitor, resources)

    val exception1 = new RuntimeException
    when(resource1.close()).thenThrow(exception1)
    val exception2 = new RuntimeException
    when(resource2.close()).thenThrow(exception2)

    val throwable = Try(resources.close(success = true)).failed.get
    close(resource1, monitor)
    close(resource2, monitor)
    close(resource3, monitor)
    verifyNoMoreInteractions(resource1, resource2, resource3, monitor)
    Set(throwable) ++ throwable.getSuppressed shouldBe Set(exception1, exception2)
  }

  private def trace(resource: AutoCloseable, monitor: ResourceMonitor, resources: ResourceManager): Unit = {
    resources.trace(resource)
    verify(monitor).trace(resource)
  }

  private def close(resource: AutoCloseable, monitor: ResourceMonitor): Unit = {
    verify(monitor).close(resource)
    verify(resource).close()
  }
}
