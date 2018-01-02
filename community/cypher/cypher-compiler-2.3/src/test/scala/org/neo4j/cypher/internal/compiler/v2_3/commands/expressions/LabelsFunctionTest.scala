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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node

class LabelsFunctionTest extends CypherFunSuite {

  test("testIdLookup") {
    // GIVEN
    val node = mock[Node]
    val queryContext = mock[QueryContext]
    val ids = Seq(12)
    when(queryContext.getLabelsForNode(node.getId)).then(new Answer[Iterator[Int]]() {
      def answer(invocation: InvocationOnMock): Iterator[Int] = ids.iterator
    })
    when(queryContext.getLabelName(12)).thenReturn("bambi")
    val state = QueryStateHelper.emptyWith(query = queryContext)
    val ctx = ExecutionContext() += ("n" -> node)

    // WHEN
    val result = LabelsFunction(Identifier("n"))(ctx)(state)

    // THEN
    result should equal(Seq("bambi"))
  }
}
