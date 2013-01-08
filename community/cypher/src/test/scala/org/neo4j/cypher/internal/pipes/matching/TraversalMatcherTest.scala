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
package org.neo4j.cypher.internal.pipes.matching

import org.junit.Test
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.graphdb.Path
import org.neo4j.cypher.internal.pipes.{MutableMaps, QueryState}
import org.neo4j.graphdb.DynamicRelationshipType.withName
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.cypher.internal.commands.True
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext
import org.neo4j.cypher.internal.ExecutionContext


class TraversalMatcherTest extends GraphDatabaseTestBase {

  val A = "A"
  val B = "B"

  val pr2 = SingleStep(1, Seq(B), OUTGOING, None, True(), True())
  val pr1 = SingleStep(0, Seq(A), OUTGOING, Some(pr2), True(), True())

  @Test def basic() {
    //Data nodes and rels
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val r1 = relate(a, b, "A")
    val r2 = relate(b, c, "B")

    val start = (_:ExecutionContext) => Seq(a)
    val end = (_:ExecutionContext) => Seq(c)

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val queryState = new QueryState(graph, new GDSBackedQueryContext(graph), Map.empty)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext(state=QueryState(graph))).toSeq

    assert(result.size === 1)
    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
    assert(result.head.lastRelationship() === r2)
  }

  @Test def tree() {
    /*Data nodes and rels
     *
     *     ->(b2)-->(c2)
     *   /
     * (a)-->(b)-->(c)
     *   \          ^
     *     \-->(b3)-|
     */
    val a = createNode("a")
    val b = createNode("b")
    val b2 = createNode("b2")
    val b3 = createNode("b3")
    val c = createNode("c")
    val c2 = createNode("c2")
    relate(a, b, "A")
    relate(a, b2, "A")
    relate(a, b3, "A")
    relate(b2, c2, "B")
    relate(b, c, "B")
    relate(b3, c, "B")

    val start = (_:ExecutionContext) => Seq(a)
    val end = (_:ExecutionContext) => Seq(c, c2)

    //Pattern nodes and relationships

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val queryState = new QueryState(graph, new GDSBackedQueryContext(graph), Map.empty)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext(state=QueryState(graph))).toSeq

    assert(result.size === 3)

    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
  }
}
