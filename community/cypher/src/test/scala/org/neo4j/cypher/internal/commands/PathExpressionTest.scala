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
package org.neo4j.cypher.internal.commands

import expressions.{Literal, Identifier, ShortestPathExpression}
import org.neo4j.cypher.GraphDatabaseTestBase
import org.scalatest.Assertions
import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.{Path, Direction}
import org.neo4j.cypher.internal.ExecutionContext
import values.LabelName
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.cypher.internal.pipes.{NullDecorator, QueryState}

class PathExpressionTest extends GraphDatabaseTestBase with Assertions {

  @Test def shouldAcceptShortestPathExpressions() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val pattern = ShortestPath(
      pathName = "p",
      start = "a",
      end = "c",
      relTypes = Seq(),
      dir = Direction.OUTGOING,
      maxDepth = None,
      optional = false,
      single = true,
      relIterator = None)

    val expression = ShortestPathExpression(pattern)

    val m = ExecutionContext.from("a" -> a, "c" -> c)

    val result = expression(m)(state).asInstanceOf[Seq[Path]].head

    assertEquals(result.startNode(), a)
    assertEquals(result.endNode(), c)
    assertEquals(result.length(), 2)
  }

  @Test def should_handle_expressions_with_labels() {
    // GIVEN
    val a = createNode()
    val b = createLabeledNode("Tror_Inte_Det")
    val c = createLabeledNode("Tror_Inte_Det2")

    relate(a, b)
    relate(a, c)

    val pattern = RelatedTo("a", "  UNNAMED1", "  UNNAMED2", Seq.empty, Direction.OUTGOING, false)
    val pred = HasLabel(Identifier("  UNNAMED1"), Seq(LabelName("Tror_Inte_Det")))
    val expression = PathExpression(Seq(pattern), pred)
    val m = createExecutionContext(Map("a" -> a))

    // WHEN
    val result: Seq[Path] = expression(m)(state).asInstanceOf[Seq[Path]]

    // THEN
    assert(result.size === 1)
  }

  private def state = {
    val ctx = new TransactionBoundQueryContext(graph)
    new QueryState(graph, ctx, Map.empty, NullDecorator)
  }

  private def createExecutionContext(m: Map[String, Any]):ExecutionContext = {
    ExecutionContext().newFrom(m)
  }
}