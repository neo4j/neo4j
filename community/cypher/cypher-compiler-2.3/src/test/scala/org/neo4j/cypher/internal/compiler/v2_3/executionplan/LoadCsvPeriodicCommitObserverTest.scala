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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.net.URL

import org.mockito.Matchers
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.ExternalResource
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class LoadCsvPeriodicCommitObserverTest extends CypherFunSuite {

  var resourceUnderTest: LoadCsvPeriodicCommitObserver = _
  var queryContext: QueryContext = _
  var resource: ExternalResource = _
  val url: URL = new URL("file:///tmp/something.csv")

  test("should not trigger tx restart until after first batch has been processed") {
    // Given
    when(resource.getCsvIterator(Matchers.eq(url), Matchers.any(), Matchers.anyBoolean())).thenReturn(Iterator(
      Array("Row1"),
      Array("Row2")))

    val iterator = resourceUnderTest.getCsvIterator(url)

    // Then
    iterator.next() should equal(Array("Row1"))
    verify(queryContext, never()).commitAndRestartTx()

    iterator.next() should equal(Array("Row2"))
    verify(queryContext, times(1)).commitAndRestartTx()
  }

  test("headers should not count") {
    // given
    when(resource.getCsvIterator(Matchers.eq(url), Matchers.any(), Matchers.eq(true))).thenReturn(Iterator(
      Array("header"),
      Array("Row1"),
      Array("Row2"),
      Array("Row3")))

    // when
    val iterator = resourceUnderTest.getCsvIterator(url, headers = true)
    verify(queryContext, never()).commitAndRestartTx()

    iterator.next() should equal(Array("header"))
    verify(queryContext, never()).commitAndRestartTx()

    iterator.next() should equal(Array("Row1"))
    verify(queryContext, never()).commitAndRestartTx()

    iterator.next() should equal(Array("Row2"))
    verify(queryContext, times(1)).commitAndRestartTx()

    iterator.next() should equal(Array("Row3"))
    verify(queryContext, times(2)).commitAndRestartTx()
  }

  test("multiple iterators are still handled correctly only commit when the first iterator advances") {
    // Given
    when(resource.getCsvIterator(Matchers.eq(url), Matchers.any(), Matchers.anyBoolean())).
      thenReturn(Iterator(Array("outer1"),Array("outer2"))).
      thenReturn(Iterator(Array("inner1"),Array("inner2"),Array("inner3"),Array("inner4")))
    val iterator1 = resourceUnderTest.getCsvIterator(url)
    val iterator2 = resourceUnderTest.getCsvIterator(url)

    // When
    iterator1.next()
    iterator2.next()
    iterator2.next()
    iterator2.next()

    verify(queryContext, never()).commitAndRestartTx()

    iterator1.next()
    verify(queryContext, times(1)).commitAndRestartTx()
  }

  test("if a custom separator is specified it should be passed to the wrapped resource") {
    // Given
    resourceUnderTest.getCsvIterator(url, Some(";"))

    // When
    verify(resource, times(1)).getCsvIterator(url, Some(";"), false)
  }

  override protected def beforeEach() {
    queryContext = mock[QueryContext]
    resource = mock[ExternalResource]
    resourceUnderTest = new LoadCsvPeriodicCommitObserver(1, resource, queryContext)
  }
}
