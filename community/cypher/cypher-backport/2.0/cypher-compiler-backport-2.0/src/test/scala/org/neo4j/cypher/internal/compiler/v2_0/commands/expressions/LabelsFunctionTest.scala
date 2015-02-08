/**
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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.{NullDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v2_0.spi.QueryContext
import org.neo4j.graphdb.Node
import org.scalatest.Assertions
import org.junit.Test
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

class LabelsFunctionTest extends Assertions with MockitoSugar {

  @Test
  def testIdLookup() {
    // GIVEN
    val node = mock[Node]
    val queryContext = mock[QueryContext]
    val ids = Seq(12)
    when(queryContext.getLabelsForNode(node.getId)).then(new Answer[Iterator[Int]]() {
      def answer(invocation: InvocationOnMock): Iterator[Int] = ids.iterator
    })
    when(queryContext.getLabelName(12)).thenReturn("bambi")
    val state = new QueryState(null, queryContext, Map.empty, NullDecorator)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = LabelsFunction(Identifier("n"))(ctx)(state)

    // THEN
    assert(Seq("bambi") === result)
  }
}
