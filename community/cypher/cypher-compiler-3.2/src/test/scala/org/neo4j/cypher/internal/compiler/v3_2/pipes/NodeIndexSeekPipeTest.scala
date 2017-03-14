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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.mockito.Matchers
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{ListLiteral, Literal, Variable}
import org.neo4j.cypher.internal.compiler.v3_2.commands.{CompositeQueryExpression, ManyQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.{CypherFunSuite, WindowsStringSafe}
import org.neo4j.cypher.internal.frontend.v3_2.{CypherTypeException, LabelId, PropertyKeyId}
import org.neo4j.graphdb.Node

class NodeIndexSeekPipeTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val monitor = mock[PipeMonitor]
  implicit val windowsSafe = WindowsStringSafe

  val label = LabelToken(LabelName("LabelName") _, LabelId(11))
  val propertyKey = Seq(PropertyKeyToken(PropertyKeyName("PropertyName") _, PropertyKeyId(10)))
  val descriptor = IndexDescriptor(label.nameId.id, propertyKey.map(_.nameId.id))
  val node = mock[Node]
  val node2 = mock[Node]

  test("should return nodes found by index lookup when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(Seq("hello") -> Iterator(node))
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, SingleQueryExpression(Literal("hello")))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle index lookups for multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        Seq("hello") -> Iterator(node),
        Seq("world") -> Iterator(node2)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node2))
  }

  test("should handle unique index lookups for multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        Seq("hello") -> Iterator(node),
        Seq("world") -> Iterator(node2)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(ListLiteral(Literal("hello"), Literal("world"))), UniqueIndexSeek)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node2))
  }

  test("should handle index lookups for multiple values when some are null") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        Seq("hello") -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(
      ListLiteral(
        Literal("hello"),
        Literal(null))))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle unique index lookups for multiple values when some are null") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        Seq("hello") -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(
      ListLiteral(
        Literal("hello"),
        Literal(null))), UniqueIndexSeek)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle index lookups for IN an empty collection") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = indexFor(
        Seq("hello") -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(ListLiteral()))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List.empty)
  }

  test("should handle index lookups for IN a collection with duplicates") {
    // given
    val queryState = QueryStateHelper.emptyWith(// WHERE n.prop IN ['hello', 'hello']
      query = indexFor(
        Seq("hello") -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(ListLiteral(
      Literal("hello"),
      Literal("hello")
    )))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle index lookups for IN a collection that returns the same nodes for multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(// WHERE n.prop IN ['hello', 'hello']
      query = indexFor(
        Seq("hello") -> Iterator(node),
        Seq("world") -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(ListLiteral(
      Literal("hello"),
      Literal("world")
    )))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node))
  }

  test("should handle index lookups for composite index lookups over multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(// WHERE n.prop = 'hello' AND n.prop2 = 'world']
      query = indexFor(
        Seq("hello", "world") -> Iterator(node),
        Seq("hello") -> Iterator(node, node2)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label,
      propertyKey :+ PropertyKeyToken(PropertyKeyName("prop2") _, PropertyKeyId(11)),
      CompositeQueryExpression(Seq(
        SingleQueryExpression(Literal("hello")),
        SingleQueryExpression(Literal("world"))
      )))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should give a helpful error message") {
    // given
    val queryState = QueryStateHelper.empty

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Literal("wut?")))()

    // then
    intercept[CypherTypeException](pipe.createResults(queryState))
  }

  test("should return the node found by the unique index lookup when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(query = indexFor(Seq("hello") -> Iterator(node)))

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, SingleQueryExpression(Literal("hello")), UniqueIndexSeek)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should use existing values from arguments when available") {
    //  GIVEN "hello" as x MATCH a WHERE a.prop = x
    val queryState: QueryState = QueryStateHelper.emptyWith(
      query = indexFor(Seq("hello") -> Iterator(node)),
      initialContext = Some(ExecutionContext.from("x" -> "hello"))
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, SingleQueryExpression(Variable("x")))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  private def indexFor(values: (Seq[Any], Iterator[Node])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.indexSeek(any(), any())).thenReturn(Iterator.empty)

    values.foreach {
      case (searchTerm, result) => when(query.indexSeek(any(), Matchers.eq(searchTerm))).thenReturn(result)
    }

    query
  }
}
