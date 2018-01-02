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
package org.neo4j.cypher.internal.compatibility

import java.util.NoSuchElementException

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_2.PlannerName
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{Node, Relationship, ResourceIterator}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}

import scala.collection.mutable.ListBuffer

class ExecutionResultWrapperFor2_2Test extends CypherFunSuite {

  test("visitor get works") {
    val n = mock[Node]
    val r = mock[Relationship]
    val objectUnderTest = createInnerExecutionResult("a", "a", n, r)

    val visitor = new CapturingResultVisitor(_.get("a"))
    objectUnderTest.accept(visitor)

    visitor.results should contain allOf("a", n, r)
  }

  test("visitor get string works") {
    val objectUnderTest = createInnerExecutionResult("a", "a", "b", "c")

    val visitor = new CapturingResultVisitor(_.getString("a"))
    objectUnderTest.accept(visitor)

    visitor.results should contain allOf("a", "b", "c")
  }

  test("visitor get node works") {
    val n1 = mock[Node]
    val n2 = mock[Node]
    val n3 = mock[Node]
    val objectUnderTest = createInnerExecutionResult("a", n1, n2, n3)

    val visitor = new CapturingResultVisitor(_.getNode("a"))
    objectUnderTest.accept(visitor)

    visitor.results should contain allOf(n1, n2, n3)
  }

  test("when asking for a node when it is not a node") {
    val objectUnderTest = createInnerExecutionResult("a", Long.box(42))

    val visitor = new CapturingResultVisitor(_.getNode("a"))

    try {
      objectUnderTest.accept(visitor)
      fail("Should have thrown " + classOf[NoSuchElementException])
    }
    catch {
      case e: NoSuchElementException =>
        e.getMessage should be("The current item in column \"a\" is not a " + classOf[Node] + ": \"42\"")
    }
  }

  test("when asking for a non existing column throws") {
    val objectUnderTest = createInnerExecutionResult("a", Int.box(42))

    val visitor = new CapturingResultVisitor(_.getNode("does not exist"))

    try {
      objectUnderTest.accept(visitor)
      fail("Should have thrown " + classOf[IllegalArgumentException])
    }
    catch {
      case e: IllegalArgumentException =>
        e.getMessage should be("No column \"does not exist\" exists")
    }
  }

  test("when asking for a rel when it is not a rel") {
    val objectUnderTest = createInnerExecutionResult("a", Long.box(42))

    val visitor = new CapturingResultVisitor(_.getRelationship("a"))

    try {
      objectUnderTest.accept(visitor)
      fail("Should have thrown " + classOf[NoSuchElementException])
    }
    catch {
      case e: NoSuchElementException =>
        e.getMessage should be("The current item in column \"a\" is not a " + classOf[Relationship] + ": \"42\"")
    }
  }

  test("null key gives a friendly error") {
    val objectUnderTest = createInnerExecutionResult("a", Int.box(42))

    val visitor = new CapturingResultVisitor(_.getNumber(null))

    try {
      objectUnderTest.accept(visitor)
      fail("Should have thrown " + classOf[IllegalArgumentException])
    }
    catch {
      case e: IllegalArgumentException =>
        e.getMessage should be("No column \"null\" exists")
    }
  }

  test("when asking for a null value nothing bad happens") {
    val objectUnderTest = createInnerExecutionResult("a", null)

    val visitor = new CapturingResultVisitor(_.getRelationship("a"))

    objectUnderTest.accept(visitor)
    visitor.results should be(List(null))
  }

  test("stop on return false") {
    val objectUnderTest = createInnerExecutionResult("a", Long.box(1), Long.box(2), Long.box(3), Long.box(4))

    val result = ListBuffer[Any]()
    val visitor = new ResultVisitor[RuntimeException] {
      override def visit(row: ResultRow) = {
        result += row.getNumber("a")
        false
      }
    }

    objectUnderTest.accept(visitor)
    result should contain only 1
  }

  test("no unnecessary object creation") {
    val objectUnderTest = createInnerExecutionResult("a", Long.box(1), Long.box(2))

    val visitor = new CapturingResultVisitor(_.hashCode())

    objectUnderTest.accept(visitor)
    visitor.results.toSet should have size 1
  }

  test("no outofbounds on empty result") {
    val objectUnderTest = createInnerExecutionResult("a")

    val visitor = new CapturingResultVisitor(_ => {
      fail("visit should never be called on empty result")
    })

    objectUnderTest.accept(visitor)
  }

  test("get boolean should return correct type") {
    val objectUnderTest = createInnerExecutionResult("a", java.lang.Boolean.TRUE)

    val visitor = new CapturingResultVisitor(_.getBoolean("a"))
    objectUnderTest.accept(visitor)

    visitor.results should contain only (java.lang.Boolean.TRUE)
  }

  private def createInnerExecutionResult(column: String, values: AnyRef*) = {
    val mockObj = mock[InternalExecutionResult]
    var offset = 0
    when(mockObj.hasNext).thenAnswer(new Answer[Boolean] {
      override def answer(invocationOnMock: InvocationOnMock) = offset < values.length
    })
    when(mockObj.next()).thenAnswer(new Answer[Map[String, AnyRef]] {
      override def answer(invocationOnMock: InvocationOnMock) = {
        val result = Map(column -> values(offset))
        offset += 1
        result
      }
    })
    when(mockObj.javaIterator).thenReturn(mock[ResourceIterator[java.util.Map[String, Any]]])
    new ExecutionResultWrapperFor2_2(mockObj, mock[PlannerName])(mock[QueryExecutionMonitor], mock[QuerySession])
  }

  private class CapturingResultVisitor(f: ResultRow => Any) extends ResultVisitor[RuntimeException] {

    val results = ListBuffer[Any]()

    override def visit(row: ResultRow): Boolean = {
      results += f(row)
      true
    }
  }

}


