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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.spi.{Operations, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Literal
import org.neo4j.cypher.internal.compiler.v2_1.{PropertyKeyId, LabelId}
import org.neo4j.graphdb.Node
import org.mockito.Mockito
import org.neo4j.kernel.api.index.IndexDescriptor

class NodeIndexScanPipeTest extends CypherFunSuite {

  test("should return nodes found by index lookup when both labelId and property key id are solved at compile time") {
    // given
    val node = mock[Node]
    val descriptor = new IndexDescriptor(11, 10)
    val queryState = QueryStateHelper.emptyWith(
      query = Mockito.when(mock[QueryContext].exactIndexSearch(descriptor,"hello")).thenReturn(Iterator(node)).getMock[QueryContext]
    )

    // when
    val pipe = NodeIndexScanPipe("n", Right(LabelId(11)), Right(PropertyKeyId(10)), Literal("hello"))
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should return nodes found by index lookup when labelId is resolved at compile time and property key id is solved at runtime") {
    // given
    val node = mock[Node]
    val query = mock[QueryContext]
    Mockito.when(query.getOptPropertyKeyId("prop")).thenReturn(Some(10))
    val descriptor = new IndexDescriptor(11, 10)
    Mockito.when(query.exactIndexSearch(descriptor,"hello")).thenReturn(Iterator(node))
    val queryState = QueryStateHelper.emptyWith(query = query)

    // when
    val pipe = NodeIndexScanPipe("n", Right(LabelId(11)), Left("prop"), Literal("hello"))
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should return nodes found by index lookup when labelId is resolved at runtime and property key id is solved at compile time") {
    // given
    val node = mock[Node]
    val query = mock[QueryContext]
    Mockito.when(query.getOptLabelId("label")).thenReturn(Some(11))
    val descriptor = new IndexDescriptor(11, 10)
    Mockito.when(query.exactIndexSearch(descriptor,"hello")).thenReturn(Iterator(node))
    val queryState = QueryStateHelper.emptyWith(query = query)

    // when
    val pipe = NodeIndexScanPipe("n", Left("label"), Right(PropertyKeyId(10)), Literal("hello"))
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should return nodes found by index lookup when both labelId and property key id is solved at runtime") {
    // given
    val node = mock[Node]
    val query = mock[QueryContext]
    Mockito.when(query.getOptLabelId("label")).thenReturn(Some(11))
    Mockito.when(query.getOptPropertyKeyId("prop")).thenReturn(Some(10))
    val descriptor = new IndexDescriptor(11, 10)
    Mockito.when(query.exactIndexSearch(descriptor, 42)).thenReturn(Iterator(node))
    val queryState = QueryStateHelper.emptyWith(query = query)

    // when
    val pipe = NodeIndexScanPipe("n", Left("label"), Left("prop"), Literal(42))
    val result = pipe.createResults(queryState)

    // then
    result.map(_("n")).toList should equal(List(node))
  }

  test("should return empty iterator when labelId cannot be resolved") {
    // given
    val query = mock[QueryContext]
    Mockito.when(query.getOptLabelId("label")).thenReturn(None)
    val queryState = QueryStateHelper.emptyWith(query = query)

    // when
    val pipe = NodeIndexScanPipe("n", Left("label"), Right(PropertyKeyId(10)), Literal("hello"))
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
  }

  test("should return empty iterator when property key id cannot be resolved") {
    // given
    val query = mock[QueryContext]
    Mockito.when(query.getOptPropertyKeyId("prop")).thenReturn(None)
    val queryState = QueryStateHelper.emptyWith(query = query)

    // when
    val pipe = NodeIndexScanPipe("n", Right(LabelId(10)), Left("prop"), Literal("hello"))
    val result = pipe.createResults(queryState)

    // then
    result should be(empty)
  }
}
