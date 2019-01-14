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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

import scala.util.Try

class ResourceManagerTest extends CypherFunSuite {

  test("should be able to trace and release a resource") {
    val resource = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource)

    resources.release(resource)
    verify(resource).close()

    resources.close(success = true)
    verifyNoMoreInteractions(resource)
  }

  test("should close the unreleased resource when closed") {
    val resource = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource)

    resources.close(success = true)
    verify(resource).close()
    verifyNoMoreInteractions(resource)
  }

  test("should not close resources multiple times when closed") {
    val resource = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource)

    resources.close(success = true)
    verify(resource).close()
    resources.close(success = true)
    verifyNoMoreInteractions(resource)
  }

  test("should be able to trace and release multiple resources") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource1)
    resources.trace(resource2)

    resources.release(resource1)
    verify(resource1).close()
    resources.release(resource2)
    verify(resource2).close()

    resources.close(success = true)
    verifyNoMoreInteractions(resource1)
    verifyNoMoreInteractions(resource2)
  }

  test("should close the unreleased resources when closed") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource1)
    resources.trace(resource2)

    resources.close(success = true)
    verify(resource1).close()
    verify(resource2).close()
    verifyNoMoreInteractions(resource1, resource2)
  }

  test("should close only the unreleased resources when closed") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource1)
    resources.trace(resource2)

    resources.release(resource2)
    verify(resource2).close()

    resources.close(success = true)
    verify(resource1).close()
    verifyNoMoreInteractions(resource1, resource2)
  }

  test("should close all the resources even in case of exceptions") {
    val resource1 = mock[AutoCloseable]
    val resource2 = mock[AutoCloseable]
    val resource3 = mock[AutoCloseable]
    val resources = new ResourceManager
    resources.trace(resource1)
    resources.trace(resource2)
    resources.trace(resource3)

    val exception1 = new RuntimeException
    when(resource1.close()).thenThrow(exception1)
    val exception2 = new RuntimeException
    when(resource2.close()).thenThrow(exception2)

    val throwable = Try(resources.close(success = true)).failed.get
    verify(resource1).close()
    verify(resource2).close()
    verify(resource3).close()
    verifyNoMoreInteractions(resource1, resource2, resource3)
    Set(throwable) ++ throwable.getSuppressed shouldBe Set(exception1, exception2)
  }
}
