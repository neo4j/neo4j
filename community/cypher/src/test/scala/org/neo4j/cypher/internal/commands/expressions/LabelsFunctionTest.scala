/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.cypher.internal.commands.values.LabelId
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.graphdb.Node
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.spi.QueryContext
import org.mockito.Mockito

class LabelsFunctionTest extends Assertions with MockitoSugar {

  @Test
  def testIdLookup() {
    // GIVEN
    val node = mock[Node]
    val queryContext = mock[QueryContext]
    val ids = Seq(12L)
    Mockito.when(queryContext.getLabelsForNode(node.getId)).thenReturn(ids)
    val state = new QueryState(null, queryContext, Map.empty)
    val ctx = ExecutionContext(state = state) += ("n" -> node)

    // WHEN
    val when: Expression = LabelsFunction(Identifier("n"))

    // THEN
    assert(when(ctx) === Seq(LabelId(12L)))
  }
}