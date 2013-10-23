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
package org.neo4j.cypher.internal.compiler.v2_0.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.True
import pipes._
import org.neo4j.cypher.GraphDatabaseTestBase
import org.neo4j.graphdb.{Direction, Node, Path}
import org.neo4j.graphdb.Direction.OUTGOING
import org.junit.Test

class TraversalMatcherTest extends GraphDatabaseTestBase {

  val A = "A"
  val B = "B"

  val pr2 = SingleStep(1, Seq(B), OUTGOING, None, True(), True())
  val pr1 = SingleStep(0, Seq(A), OUTGOING, Some(pr2), True(), True())

  def queryState = QueryStateHelper.queryStateFrom(graph)

  @Test def basic() {
    //Data nodes and rels
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "A")
    val r2 = relate(b, c, "B")

    val start = createStartPointIterator(a)
    val end = createStartPointIterator(c)

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext()).toSeq

    assert(result.size === 1)
    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
    assert(result.head.lastRelationship() === r2)
  }

  private def createStartPointIterator(x: Node*) = EntityProducer[Node]("Produce") {
    (_: ExecutionContext, _: QueryState) => x.iterator
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

    val start = createStartPointIterator(a)
    val end = createStartPointIterator(c, c2)

    //Pattern nodes and relationships

    val matcher = new BidirectionalTraversalMatcher(pr1, start, end)

    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext()).toSeq

    assert(result.size === 3)

    assert(result.head.startNode() === a)
    assert(result.head.endNode() === c)
  }

  @Test def fullUndirected2NodeGraph() {
    val nodeA = createNode("a")
    val nodeB = createNode("b")

    relate(nodeA, nodeB, "LINK")

    val start = createStartPointIterator(nodeA, nodeB)
    val end = createStartPointIterator(nodeA, nodeB)

    val pr = SingleStep(0, Seq("LINK"), Direction.BOTH, None, True(), True())
    val matcher = new BidirectionalTraversalMatcher(pr, start, end)

    val result: Set[(Long, Long)] =
      matcher
        .findMatchingPaths(queryState, ExecutionContext()).map((p: Path) => (p.startNode().getId, p.endNode().getId))
        .toSet

    val a = nodeA.getId
    val b = nodeB.getId

    assert( Set((a, b), (b, a)) === result )
  }

  @Test def fullUndirected3NodeGraph() {
    val nodeA = createNode("a")
    val nodeB = createNode("b")
    val nodeC = createNode("c")

    relate(nodeA, nodeB, "LINK")
    relate(nodeC, nodeA, "LINK")
    relate(nodeB, nodeC, "LINK")

    val start = createStartPointIterator(nodeA, nodeB, nodeC)
    val end = createStartPointIterator(nodeA, nodeB, nodeC)

    val pr = SingleStep(0, Seq("LINK"), Direction.BOTH, None, True(), True())
    val matcher = new BidirectionalTraversalMatcher(pr, start, end)

    val result: Set[(Long, Long)] =
      matcher
        .findMatchingPaths(queryState, ExecutionContext()).map((p: Path) => (p.startNode().getId, p.endNode().getId))
        .toSet


    val a = nodeA.getId
    val b = nodeB.getId
    val c = nodeC.getId

    assert(Set((a, b), (a, c), (b, a), (b, c), (c, a), (c, b)) === result)
  }

  @Test def should_not_return_paths_that_traverse_the_same_graph_relationship_multiple_times() {
    // Given MATCH a-->b-->c-->d
    val pr3 = SingleStep(id = 3, typ = Seq.empty, direction = OUTGOING, next = None, relPredicate = True(), nodePredicate = True())
    val pr2 = SingleStep(id = 2, typ = Seq.empty, direction = OUTGOING, next = Some(pr3), relPredicate = True(), nodePredicate = True())
    val pr1 = SingleStep(id = 1, typ = Seq.empty, direction = OUTGOING, next = Some(pr2), relPredicate = True(), nodePredicate = True())
    val pr0 = SingleStep(id = 0, typ = Seq.empty, direction = OUTGOING, next = Some(pr1), relPredicate = True(), nodePredicate = True())

    val n0 = createNode("n0")
    val n1 = createNode("n1")

    relate(n0, n1, "r0")
    relate(n1, n0, "r1")

    val start = createStartPointIterator(n0)

    // When
    val matcher = new MonoDirectionalTraversalMatcher(pr0, start)
    val result: Seq[Path] = matcher.findMatchingPaths(queryState, ExecutionContext()).toSeq

    // Then
    // If there were no uniqueness constraint then this path would match the pattern:
    //   (n0)-[r0]->(n1)-[r1]->(n0)-[r0]->(n1)
    // but this traverses r0 twice, so it should be excluded.
    assert(result.size === 0)
  }
}
