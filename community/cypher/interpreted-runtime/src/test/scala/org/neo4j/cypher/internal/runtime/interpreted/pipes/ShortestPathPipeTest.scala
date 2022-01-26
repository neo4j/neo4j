/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyLong
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.Expander
import org.neo4j.cypher.internal.runtime.KernelPredicate
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.ShortestPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.SingleNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.memory.MemoryTracker

import java.util.Collections

class ShortestPathPipeTest extends CypherFunSuite {
  test("should be lazy") {
    val shortestPath = ShortestPath(pathName = "p", left = SingleNode("start"), right = SingleNode("end"), relTypes = Seq.empty,
      dir = SemanticDirection.OUTGOING, allowZeroLength = true, maxDepth = None, single = true, relIterator = None)
    val n1 = mock[Node]
    when(n1.getRelationships).thenReturn(Iterables.emptyResourceIterable[Relationship]())
    val input = new FakePipe(Seq.fill(10)(Map("start" -> n1, "end" -> n1)))
    val pipe = ShortestPathPipe(input, ShortestPathExpression(shortestPath))()
    val context = mock[QueryContext](Mockito.RETURNS_DEEP_STUBS)
    val p = mock[Path]
    when(p.nodes()).thenReturn(java.util.List.of[Node](n1))
    when(p.relationships()).thenReturn(Collections.emptyList[Relationship]())
    when(context.singleShortestPath(anyLong(), anyLong(), anyInt(), any[Expander], any[KernelPredicate[Path]], any[Seq[KernelPredicate[Entity]]], any[MemoryTracker]))
      .thenReturn(Some(p))

    // when
    val res = pipe.createResults(QueryStateHelper.emptyWith(query = context))
    res.next()
    // then
    input.numberOfPulledRows shouldBe 1
  }
}
