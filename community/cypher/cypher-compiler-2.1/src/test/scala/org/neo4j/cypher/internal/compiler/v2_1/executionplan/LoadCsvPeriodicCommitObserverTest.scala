/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_1.pipes.ExternalResource
import java.net.URL
import org.mockito.Mockito._

class LoadCsvPeriodicCommitObserverTest extends CypherFunSuite {

  var resourceUnderTest: LoadCsvPeriodicCommitObserver = _
  var queryContext: QueryContext = _
  var resource: ExternalResource = _
  val url: URL = new URL("file:///tmp/something.csv")

  test("reading and not writing does not commit anything") {
    when(resource.getCsvIterator(url)).thenReturn(Iterator(Array("yo")))

    // Get iterator and exhaust it
    resourceUnderTest.getCsvIterator(url).size

    verify(queryContext, never()).commitAndRestartTx()
    verify(resource, times(1)).getCsvIterator(url)
  }

  test("writing should not trigger tx restart until next csv line is fetched") {
    // Given
    when(resource.getCsvIterator(url)).thenReturn(Iterator(Array("yo")))

    // When
    val iterator = resourceUnderTest.getCsvIterator(url)
    resourceUnderTest.notify(12)
    verify(queryContext, never()).commitAndRestartTx()

    iterator.next()

    verify(queryContext, times(1)).commitAndRestartTx()
  }

  test("writing should trigger tx restart when there are more then double batch size updates in one shot during a line processing") {
    // Given
    when(resource.getCsvIterator(url)).thenReturn(Iterator(Array("yo")))

    // When
    val iterator = resourceUnderTest.getCsvIterator(url)
    resourceUnderTest.notify(30)
    verify(queryContext, times(1)).commitAndRestartTx()

    iterator.next()

    verify(queryContext, times(1)).commitAndRestartTx()
  }

  test("writing should trigger tx restart when there are more then double batch size updates notify multiple times during a line processing") {
    // Given
    when(resource.getCsvIterator(url)).thenReturn(Iterator(Array("yo")))

    // When
    val iterator = resourceUnderTest.getCsvIterator(url)
    resourceUnderTest.notify(25)
    verify(queryContext, times(1)).commitAndRestartTx()
    resourceUnderTest.notify(5)
    resourceUnderTest.notify(6)

    iterator.next()

    verify(queryContext, times(2)).commitAndRestartTx()
  }

  test("multiple iterators are still handled correctly only commit when the first iterator advances") {
    // Given
    when(resource.getCsvIterator(url)).
      thenReturn(Iterator(Array("yo"))).
      thenReturn(Iterator(Array("yo")))
    val iterator1 = resourceUnderTest.getCsvIterator(url)
    val iterator2 = resourceUnderTest.getCsvIterator(url)

    // When
    resourceUnderTest.notify(12)
    iterator2.next()

    verify(queryContext, never()).commitAndRestartTx()

    iterator1.next()
    verify(queryContext, times(1)).commitAndRestartTx()
  }

  override protected def beforeEach() {
    queryContext = mock[QueryContext]
    resource = mock[ExternalResource]
    resourceUnderTest = new LoadCsvPeriodicCommitObserver(10, resource, queryContext)
  }
}
