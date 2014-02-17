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

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.neo4j.cypher.internal.compiler.v2_1.spi.{Operations, QueryContext}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.graphdb.{Relationship, Node}

@RunWith(classOf[JUnitRunner])
class PeriodicCommitQueryContextTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  implicit def intWithTimes(n: Int) = new {
    def times(f: => Unit) = 1 to n foreach {
      _ => f
    }
  }


  var inner: QueryContext = _
  var innerNodeOpds: Operations[Node] = _
  var innerRelOpds: Operations[Relationship] = _

  var contextUnderTest: PeriodicCommitQueryContext = _

  before {
    inner = mock[QueryContext]
    innerNodeOpds = mock[Operations[Node]]
    innerRelOpds = mock[Operations[Relationship]]
    when(inner.nodeOps).thenReturn(innerNodeOpds)
    when(inner.relationshipOps).thenReturn(innerRelOpds)
    contextUnderTest = new PeriodicCommitQueryContext(3, inner)
  }

  test("should pass through 100 reads") {
    100 times {
      contextUnderTest.getLabelName(42)
    }

    verify(inner, times(100)).getLabelName(42)
    verify(inner, never()).commitAndRestartTx()
  }

  test("should pass through a createNode") {
    contextUnderTest.createNode()

    verify(inner, times(1)).createNode()
    verify(inner, never()).commitAndRestartTx()
  }

  test("if called 3 times, passed on 3 times and commits once") {
    3 times {
      contextUnderTest.createNode()
    }

    verify(inner, times(3)).createNode()
    verify(inner, times(1)).commitAndRestartTx()
  }

  test("if called 4 times, passed on 4 times and commits once (for node ops)") {
    val node = mock[Node]

    4 times {
      contextUnderTest.nodeOps.delete(node)
    }

    verify(innerNodeOpds, times(4)).delete(node)
    verify(inner, times(1)).commitAndRestartTx()
  }

  test("if called 4 times, passed on 4 times and commits once (for relationship ops)") {
    val node = mock[Node]

    4 times {
      contextUnderTest.nodeOps.delete(node)
    }

    verify(innerNodeOpds, times(4)).delete(node)
    verify(inner, times(1)).commitAndRestartTx()
  }

  test("if called 300 times, passed on 300 times and commits 100 times") {
    300 times {
      contextUnderTest.createNode()
    }

    verify(inner, times(300)).createNode()
    verify(inner, times(100)).commitAndRestartTx()
  }

}
