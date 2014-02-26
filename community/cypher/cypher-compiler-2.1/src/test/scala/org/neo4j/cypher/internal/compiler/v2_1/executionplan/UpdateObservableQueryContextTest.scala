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
import org.neo4j.cypher.internal.compiler.v2_1.spi.{Operations, QueryContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.graphdb.{Relationship, Node}


class UpdateObservableQueryContextTest extends CypherFunSuite {

  var queryContext: QueryContext = _
  var observer: UpdateObserver = _
  var contextUnderTest: UpdateObservableQueryContext = _
  var innerNodeOps: Operations[Node] = _
  var innerRelationshipOps: Operations[Relationship] = _


  override protected def beforeEach() {
    queryContext = mock[QueryContext]
    observer = mock[UpdateObserver]
    innerNodeOps = mock[Operations[Node]]
    innerRelationshipOps = mock[Operations[Relationship]]
    when(queryContext.nodeOps).thenReturn(innerNodeOps)
    when(queryContext.relationshipOps).thenReturn(innerRelationshipOps)
    contextUnderTest = new UpdateObservableQueryContext(observer, queryContext)
  }

  implicit def intWithTimes(n: Int) = new {
    def times(f: => Unit) = 1 to n foreach {
      _ => f
    }
  }

  test("should pass through 100 reads") {
    100 times {
      contextUnderTest.getLabelName(42)
    }

    verify(queryContext, times(100)).getLabelName(42)
    verify(observer, never()).notify(any(classOf[Long]))
  }

  test("should pass through a createNode") {
    contextUnderTest.createNode()

    verify(queryContext, times(1)).createNode()
    verify(observer, times(1)).notify(1)
  }

  test("if called 3 times, passed on 3 times and commits once") {
    3 times {
      contextUnderTest.createNode()
    }

    verify(queryContext, times(3)).createNode()
    verify(observer, times(3)).notify(1)
  }

  test("if called 4 times, passed on 4 times and commits once (for node ops)") {
    val node = mock[Node]

    4 times {
      contextUnderTest.nodeOps.delete(node)
    }

    verify(innerNodeOps, times(4)).delete(node)
    verify(observer, times(4)).notify(1)
  }

  test("if called 4 times, passed on 4 times and commits once (for relationship ops)") {
    val rel = mock[Relationship]

    4 times {
      contextUnderTest.relationshipOps.delete(rel)
    }

    verify(innerRelationshipOps, times(4)).delete(rel)
    verify(observer, times(4)).notify(1)
  }

  test("if called 1 time and setting 4 labels then it should notify 1 time the observer with increment 4") {
    val iterator = Iterator(1, 2, 3, 4)
    when(queryContext.setLabelsOnNode(any(classOf[Long]), any(classOf[Iterator[Int]]))).thenReturn(4)
    contextUnderTest.setLabelsOnNode(100, iterator)

    verify(queryContext, times(1)).setLabelsOnNode(100, iterator)
    verify(observer, times(1)).notify(4)
  }


  test("if called 1 time and setting no labels then it should not notify the observer") {
    val iterator = Iterator[Int]()
    when(queryContext.setLabelsOnNode(any(classOf[Long]), any(classOf[Iterator[Int]]))).thenReturn(0)
    contextUnderTest.setLabelsOnNode(100, iterator)

    verify(queryContext, times(1)).setLabelsOnNode(100, iterator)
    verify(observer, never()).notify(any(classOf[Long]))
  }
}
