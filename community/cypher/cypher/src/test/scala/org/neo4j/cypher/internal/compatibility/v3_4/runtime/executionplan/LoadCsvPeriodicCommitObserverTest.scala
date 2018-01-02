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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import java.net.URL

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.runtime.{QueryContext, QueryTransactionalContext}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite

class LoadCsvPeriodicCommitObserverTest extends CypherFunSuite {

  var resourceUnderTest: LoadCsvPeriodicCommitObserver = _
  var transactionalContext: QueryTransactionalContext = _
  var resource: ExternalCSVResource = _
  val url: URL = new URL("file:///tmp/something.csv")

  test("writing should not trigger tx restart until next csv line is fetched") {
    // Given
    when(resource.getCsvIterator(ArgumentMatchers.eq(url), any(), any())).thenReturn(Iterator(Array("yo")))

    // When
    val iterator = resourceUnderTest.getCsvIterator(url, None, false)
    verify(transactionalContext, never()).commitAndRestartTx()

    iterator.next()

    verify(transactionalContext, times(1)).commitAndRestartTx()
  }

  test("multiple iterators are still handled correctly only commit when the first iterator advances") {
    // Given
    when(resource.getCsvIterator(ArgumentMatchers.eq(url), any(), any())).
      thenReturn(Iterator(Array("yo"))).
      thenReturn(Iterator(Array("yo")))
    val iterator1 = resourceUnderTest.getCsvIterator(url, None, false)
    val iterator2 = resourceUnderTest.getCsvIterator(url, None, false)

    // When
    iterator2.next()

    verify(transactionalContext, never()).commitAndRestartTx()

    iterator1.next()
    verify(transactionalContext, times(1)).commitAndRestartTx()
  }

  test("if a custom iterator is specified should be passed to the wrapped resource") {
    // Given
    resourceUnderTest.getCsvIterator(url, Some(";"), false)

    // When
    verify(resource, times(1)).getCsvIterator(url, Some(";"), false)
  }

  override protected def beforeEach() {
    val queryContext = mock[QueryContext]
    transactionalContext = mock[QueryTransactionalContext]
    when(queryContext.transactionalContext).thenReturn(transactionalContext)
    resource = mock[ExternalCSVResource]
    resourceUnderTest = new LoadCsvPeriodicCommitObserver(1, resource, queryContext)
  }
}
