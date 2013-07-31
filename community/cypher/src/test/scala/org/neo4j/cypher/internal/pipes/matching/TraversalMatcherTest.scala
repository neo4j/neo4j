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
import org.neo4j.graphdb.{Node, Path}
import org.neo4j.cypher.internal.pipes._
import org.neo4j.graphdb.Direction.OUTGOING
import org.neo4j.graphdb.Direction.BOTH
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState
import org.neo4j.cypher.internal.commands.True


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

    val start = produce(a)
    val end = produce(c)

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val queryState = QueryStateHelper.queryStateFrom(graph)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext()).toSeq

    assert(result.size === 1)
    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
    assert(result.head.lastRelationship() === r2)
  }

  private def produce(x: Node*) = EntityProducer[Node]("Produce") { (_: ExecutionContext, _: QueryState) => x.iterator }

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

    val start = produce(a)
    val end = produce(c, c2)

    //Pattern nodes and relationships

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val queryState = QueryStateHelper.queryStateFrom(graph)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext()).toSeq

    assert(result.size === 3)

    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
  }

  @Test def fullUndirected2NodeGraph()
  {
    val nodeA = createNode("a")
    val nodeB = createNode("b")

    relate(nodeA, nodeB, "LINK")

    val start = produce(nodeA, nodeB)
    val end = produce(nodeA, nodeB)

    val pr = SingleStep(0, Seq("LINK"), BOTH, None, True(), True())
    val matcher = new BidirectionalTraversalMatcher(pr, start, end)

    val queryState = QueryStateHelper.queryStateFrom(graph)

    val result: Set[(Long, Long)] =
      matcher
        .findMatchingPaths(queryState, ExecutionContext()).map( (p: Path) => (p.startNode().getId, p.endNode().getId ) )
        .toSet


    val a = nodeA.getId
    val b = nodeB.getId

    assert( Set((a, b), (b, a)) === result )
  }

  @Test def fullUndirected3NodeGraph()
  {
    val nodeA = createNode("a")
    val nodeB = createNode("b")
    val nodeC = createNode("c")

    relate(nodeA, nodeB, "LINK")
    relate(nodeC, nodeA, "LINK")
    relate(nodeB, nodeC, "LINK")

    val start = produce(nodeA, nodeB, nodeC)
    val end = produce(nodeA, nodeB, nodeC)

    val pr = SingleStep(0, Seq("LINK"), BOTH, None, True(), True())
    val matcher = new BidirectionalTraversalMatcher(pr, start, end)

    val queryState = QueryStateHelper.queryStateFrom(graph)

    val result: Set[(Long, Long)] =
      matcher
        .findMatchingPaths(queryState, ExecutionContext()).map( (p: Path) => (p.startNode().getId, p.endNode().getId ) )
        .toSet


    val a = nodeA.getId
    val b = nodeB.getId
    val c = nodeC.getId

    assert( Set((a, b), (a, c), (b, a), (b, c), (c, a), (c, b)) === result )
  }
}
