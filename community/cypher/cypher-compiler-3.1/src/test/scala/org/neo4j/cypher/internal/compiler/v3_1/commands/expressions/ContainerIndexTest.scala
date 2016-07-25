/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.expressions

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v3_1.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v3_1.symbols.{FakeExpression, SymbolTable}
import org.neo4j.cypher.internal.frontend.v3_1.{CypherTypeException, InvalidArgumentException}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.JavaConverters._

class ContainerIndexTest extends CypherFunSuite {

  val qtx = mock[QueryContext]
  implicit val state = QueryStateHelper.empty.withQueryContext(qtx)
  val ctx = ExecutionContext.empty
  val expectedNull: Any = null

  test("handles collection lookup") {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    idx(0) should equal(1)
    idx(1) should equal(2)
    idx(2) should equal(3)
    idx(3) should equal(4)
    idx(-1) should equal(4)
    idx(100) should equal(expectedNull)
  }

  test("handles empty collections") {
    implicit val collection = Collection()

    idx(0) should equal(expectedNull)
    idx(-1) should equal(expectedNull)
    idx(100) should equal(expectedNull)
  }

  test("handles nulls") {
    implicit val collection = Literal(null)

    idx(0) should equal(expectedNull)
  }

  test("handles scala map lookup") {
    implicit val expression = Literal(Map("a" -> 1, "b" -> "foo"))

    idx("a") should equal(1)
    idx("b") should equal("foo")
    idx("c") should equal(null.asInstanceOf[AnyRef])
  }

  test("handles java map lookup") {
    implicit val expression = Literal(Map("a" -> 1, "b" -> "foo").asJava)

    idx("a") should equal(1)
    idx("b") should equal("foo")
    idx("c") should equal(null.asInstanceOf[AnyRef])
  }

  test("handles node lookup") {
    val node = mock[Node]
    when(node.getId).thenReturn(0)
    implicit val expression = Literal(node)

    when(qtx.getOptPropertyKeyId("v")).thenReturn(Some(0))
    when(qtx.getOptPropertyKeyId("c")).thenReturn(Some(1))
    val nodeOps = mock[Operations[Node]]
    when(nodeOps.getProperty(0, 0)).thenAnswer(new Answer[Int] {
      override def answer(invocation: InvocationOnMock): Int = 1
    })
    when(qtx.nodeOps).thenReturn(nodeOps)

    idx("v") should equal(1)
    idx("c") should equal(null.asInstanceOf[AnyRef])
  }

  test("handles relationship lookup") {
    val rel = mock[Relationship]
    when(rel.getId).thenReturn(0)
    implicit val expression = Literal(rel)

    when(qtx.getOptPropertyKeyId("v")).thenReturn(Some(0))
    when(qtx.getOptPropertyKeyId("c")).thenReturn(Some(1))
    val relOps = mock[Operations[Relationship]]
    when(relOps.getProperty(0, 0)).thenAnswer(new Answer[Int] {
      override def answer(invocation: InvocationOnMock): Int = 1
    })
    when(qtx.relationshipOps).thenReturn(relOps)

    idx("v") should equal(1)
    idx("c") should equal(null.asInstanceOf[AnyRef])
  }

  test("when collection is a CTAny then type is a collection of CTAny") {
    val collection = new FakeExpression(CTAny)
    val symbols = new SymbolTable()
    val result = ContainerIndex(collection, Literal(2)).evaluateType(CTList(CTAny), symbols)

    result should equal(CTAny)
  }

  test("should fail when not integer values are passed") {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    a[CypherTypeException] should be thrownBy idx(1.0f)
    a[CypherTypeException] should be thrownBy idx(1.0d)
    a[CypherTypeException] should be thrownBy idx("bad value")
  }

  test("should fail when too big values are used to access the array") {
    implicit val collection = Literal(Seq(1, 2, 3, 4))

    val index = Int.MaxValue + 1L

    an[InvalidArgumentException] should be thrownBy idx(index)
  }

  private def idx(value: Any)(implicit collection: Expression) =
    ContainerIndex(collection, Literal(value))(ctx)(state)
}
