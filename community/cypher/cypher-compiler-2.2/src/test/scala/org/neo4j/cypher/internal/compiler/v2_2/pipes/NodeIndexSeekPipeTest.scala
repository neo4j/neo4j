/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.{ExecutionContext, CypherTypeException, LabelId, PropertyKeyId}
import org.neo4j.cypher.internal.compiler.v2_2.ast.{LabelToken, PropertyKeyToken, _}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{ManyQueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Collection, Literal, Identifier}
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.index.IndexDescriptor

class NodeIndexSeekPipeTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val monitor = mock[PipeMonitor]

  val label = LabelToken(LabelName("LabelName")_, LabelId(11))
  val propertyKey = PropertyKeyToken(PropertyKeyName("PropertyName")_, PropertyKeyId(10))
  val descriptor = new IndexDescriptor(label.nameId.id, propertyKey.nameId.id)
  val node = mock[Node]
  val node2 = mock[Node]

  test("should return nodes found by index lookup when both labelId and property key id are solved at compile time") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = exactIndexFor("hello" -> Iterator(node))
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
      query = exactIndexFor(
        "hello" -> Iterator(node ),
        "world" -> Iterator(node2)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Collection(Literal("hello"), Literal("world"))))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node2))
  }

  test("should handle unique index lookups for multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = exactUniqueIndexFor(
        "hello" -> Some(node),
        "world" -> Some(node2)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Collection(Literal("hello"), Literal("world"))), unique = true)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node2))
  }

  test("should handle index lookups for multiple values when some are null") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = exactIndexFor(
        "hello" -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(
      Collection(
        Literal("hello"),
        Literal(null))))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle unique index lookups for multiple values when some are null") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = exactUniqueIndexFor(
        "hello" -> Some(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(
      Collection(
        Literal("hello"),
        Literal(null))), unique = true)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle index lookups for IN an empty collection") {
    // given
    val queryState = QueryStateHelper.emptyWith(
      query = exactIndexFor(
        "hello" -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Collection()))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List.empty)
  }

  test("should handle index lookups for IN a collection with duplicates") {
    // given
    val queryState = QueryStateHelper.emptyWith(   // WHERE n.prop IN ['hello', 'hello']
      query = exactIndexFor(
        "hello" -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Collection(
      Literal("hello"),
      Literal("hello")
    )))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should handle index lookups for IN a collection that returns the same nodes for multiple values") {
    // given
    val queryState = QueryStateHelper.emptyWith(   // WHERE n.prop IN ['hello', 'hello']
      query = exactIndexFor(
        "hello" -> Iterator(node),
        "world" -> Iterator(node)
      )
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, ManyQueryExpression(Collection(
      Literal("hello"),
      Literal("world")
    )))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node, node))
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
    val queryState =  QueryStateHelper.emptyWith( query = exactUniqueIndexFor("hello"->Some(node)) )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, SingleQueryExpression(Literal("hello")), unique = true)()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should use existing values from arguments when available") {
    //  GIVEN "hello" as x MATCH a WHERE a.prop = x
    val queryState: QueryState = QueryStateHelper.emptyWith(
      query = exactIndexFor("hello" -> Iterator(node)) ,
      initialContext = Some(ExecutionContext.from("x" -> "hello"))
    )

    // when
    val pipe = NodeIndexSeekPipe("n", label, propertyKey, SingleQueryExpression(Identifier("x")))()
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }


  private def exactUniqueIndexFor(values : (Any, Option[Node])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.exactUniqueIndexSearch(any(), any())).thenReturn(None)

    values.foreach {
      case (searchTerm, result) => when(query.exactUniqueIndexSearch(descriptor, searchTerm)).thenReturn(result)
    }

    query
  }

  private def exactIndexFor(values : (Any, Iterator[Node])*): QueryContext = {
    val query = mock[QueryContext]
    when(query.exactIndexSearch(any(), any())).thenReturn(Iterator.empty)

    values.foreach {
      case (searchTerm, result) => when(query.exactIndexSearch(descriptor, searchTerm)).thenReturn(result)
    }

    query
  }
}
