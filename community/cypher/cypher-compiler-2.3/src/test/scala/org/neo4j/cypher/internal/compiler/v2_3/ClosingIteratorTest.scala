/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v2_3.CypherException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ClosingIteratorTest extends CypherFunSuite {
  var taskCloser: TaskCloser = _
  val exceptionDecorator: CypherException => CypherException = identity

  override def beforeEach() {
    super.beforeEach()
    taskCloser = mock[TaskCloser]
  }

  test("should_cleanup_when_we_reach_the_end") {
    //Given
    val wrapee   = Iterator(Map("k" -> 42))
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)
    //When
    val result = iterator.next()

    //Then
    verify(taskCloser).close(success = true)
    result should equal(Map[String, Any]("k" -> 42))
  }

  test("should_cleanup_even_for_empty_iterator") {
    //Given
    val wrapee   = Iterator.empty
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    val result = iterator.hasNext

    //Then
    verify(taskCloser).close(success = true)
    result should equal(false)
  }

  test("multiple_has_next_should_not_close_more_than_once") {
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
    result should equal(false)
  }

  test("exception_in_hasNext_should_fail_transaction") {
    //Given
    val wrapee = mock[Iterator[Map[String, Any]]]
    when(wrapee.hasNext).thenThrow(new RuntimeException)
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    intercept[RuntimeException](iterator.hasNext)

    //Then
    verify(taskCloser).close(success = false)
  }

  test("exception_in_next_should_fail_transaction") {
    //Given
    val wrapee = mock[Iterator[Map[String, Any]]]
    when(wrapee.hasNext).thenReturn(true)
    when(wrapee.next()).thenThrow(new RuntimeException)

    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    intercept[RuntimeException](iterator.next())

    //Then
    verify(taskCloser).close(success = false)
  }

  test("close_runs_cleanup") {
    //Given
    val wrapee   = Iterator(Map("k" -> 42), Map("k" -> 43))
    val iterator = new ClosingIterator(wrapee, taskCloser, exceptionDecorator)

    //When
    val result = iterator.next()
    iterator.close()

    //Then
    verify(taskCloser).close(success = true)
    result should equal(Map[String, Any]("k" -> 42))
  }
}
