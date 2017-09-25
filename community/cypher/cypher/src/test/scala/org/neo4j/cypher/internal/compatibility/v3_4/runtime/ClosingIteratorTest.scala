/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.mockito.ArgumentMatchers.anyBoolean
import org.mockito.Mockito._
import org.neo4j.cypher.internal.aux.v3_4.CypherException
import org.neo4j.cypher.internal.aux.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.intValue

class ClosingIteratorTest extends CypherFunSuite {
  var taskCloser: TaskCloser = _
  val exceptionDecorator: CypherException => CypherException = identity

  override def beforeEach() {
    super.beforeEach()
    taskCloser = mock[TaskCloser]
  }

  test("should not close prematurely") {
    //Given
    val wrapee   = Iterator(Map("k" -> intValue(42)))
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)
    //When
    val result = iterator.next()

    //Then
    verify(taskCloser, never()).close(anyBoolean())
    result should equal(Map[String, Any]("k" -> intValue(42)))
  }

  test("should cleanup even for empty iterator") {
    //Given
    val wrapee   = Iterator.empty
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    val result = iterator.hasNext

    //Then
    verify(taskCloser).close(success = true)
    result should equal(false)
  }

  test("multiple has next should not close more than once") {
    //Given
    val wrapee   = Iterator.empty
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    val result = iterator.hasNext
    iterator.hasNext
    iterator.hasNext
    iterator.hasNext
    iterator.hasNext

    //Then
    verify(taskCloser, atLeastOnce()).close(success = true)
    result shouldBe false
  }

  test("exception in hasNext should fail transaction") {
    //Given
    val wrapee = mock[Iterator[Map[String, AnyValue]]]
    when(wrapee.hasNext).thenThrow(new RuntimeException)
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    intercept[RuntimeException](iterator.hasNext)

    //Then
    verify(taskCloser).close(success = false)
  }

  test("exception in next should fail transaction") {
    //Given
    val wrapee = mock[Iterator[Map[String, AnyValue]]]
    when(wrapee.hasNext).thenReturn(true)
    when(wrapee.next()).thenThrow(new RuntimeException)

    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    intercept[RuntimeException](iterator.next())

    //Then
    verify(taskCloser).close(success = false)
  }

  test("close runs cleanup") {
    //Given
    val wrapee   = Iterator(Map("k" -> intValue(42)), Map("k" -> intValue(43)))
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    val result = iterator.next()
    iterator.close()

    //Then
    verify(taskCloser).close(success = true)
    result should equal(Map[String, AnyValue]("k" -> intValue(42)))
  }
}
