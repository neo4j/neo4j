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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.GraphDatabaseFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.True
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.{EndPoint, SingleStepTrail, VariableLengthStepTrail}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection

class TrailDecomposeTest extends GraphDatabaseFunSuite {
  test("decompose_simple_path") {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val rel = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB)
    val path = SingleStepTrail(EndPoint("b"), SemanticDirection.OUTGOING, "link", Seq("LINK_T"), "a", True(), True(), null, Seq())

    val resultMap = path.decompose(kernPath).toList
    resultMap should equal(List(Map("a" -> nodeA, "b" -> nodeB, "link" -> rel)))
  }

  test("decompose_little_longer_path") {
    //a-[link2]->b-[link1]->c

    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val rel1 = relate(nodeA, nodeB, "LINK_T")
    val rel2 = relate(nodeB, nodeC, "LINK_T")

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC)

    val aPoint = EndPoint("c")
    val bToA = SingleStepTrail(aPoint, SemanticDirection.OUTGOING, "link2", Seq("LINK_T"), "b", True(), True(), null, Seq())
    val cToB = SingleStepTrail(bToA, SemanticDirection.OUTGOING, "link1", Seq("LINK_T"), "a", True(), True(), null, Seq())

    val resultMap = cToB.decompose(kernPath).toList

    resultMap should equal(List(
      Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC, "link1" -> rel1, "link2" -> rel2)
    ))
  }

  test("should_not_return_maps_that_have_contradicting_values_in_pattern_points_endpoint") {
    // When a pattern has the same pattern node in multiple places in the pattern,
    // we need to exclude it from the results

    //a-[r1]->b-[r2]->c-[r3]->b

    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val nodeD = createNode("D")

    val rel1 = relate(nodeA, nodeB)
    val rel2 = relate(nodeB, nodeC)
    val rel3 = relate(nodeC, nodeD)


    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC, rel3, nodeD)

    val cPoint = EndPoint("b")
    val cToB = SingleStepTrail(cPoint, SemanticDirection.OUTGOING, "r3", Seq(), "c", True(), True(), null, Seq())
    val bToC = SingleStepTrail(cToB, SemanticDirection.OUTGOING, "r2", Seq(), "b", True(), True(), null, Seq())
    val aToB = SingleStepTrail(bToC, SemanticDirection.OUTGOING, "r1", Seq(), "a", True(), True(), null, Seq())

    val resultMap = aToB.decompose(kernPath).toList

    resultMap shouldBe empty
  }

  test("should_not_return_maps_that_have_contradicting_values_in_pattern_points_single") {
    // When a pattern has the same pattern node in multiple places in the pattern,
    // we need to exclude it from the results

    //a-[r1]->b-[r2]->c-[r3]->b-[r4]->x

    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val nodeD = createNode("D")
    val nodeX = createNode("X")

    val rel1 = relate(nodeA, nodeB)
    val rel2 = relate(nodeB, nodeC)
    val rel3 = relate(nodeC, nodeD)
    val rel4 = relate(nodeD, nodeX)

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC, rel3, nodeD, rel4, nodeX)

    val dPoint = EndPoint("x")
    val bToD = SingleStepTrail(dPoint, SemanticDirection.OUTGOING, "r4", Seq(), "b", True(), True(), null, Seq())
    val cToB = SingleStepTrail(bToD, SemanticDirection.OUTGOING, "r3", Seq(), "c", True(), True(), null, Seq())
    val bToC = SingleStepTrail(cToB, SemanticDirection.OUTGOING, "r2", Seq(), "b", True(), True(), null, Seq())
    val aToB = SingleStepTrail(bToC, SemanticDirection.OUTGOING, "r1", Seq(), "a", True(), True(), null, Seq())

    val resultMap = aToB.decompose(kernPath).toList

    resultMap shouldBe empty
  }

  test("decompose_single_varlength_step") {
    //Given:
    //Pattern: a-[:A*1..2]->b
    //   Path: 0-[:A]->1

    val nodeA = createNode()
    val nodeB = createNode()
    val rel1 = relate(nodeA, nodeB, "A")

    val kernPath = Seq(nodeA, rel1, nodeB)
    val path =
      VariableLengthStepTrail(EndPoint("b"), SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //Then
    graph.inTx {
      path.decompose(kernPath).toList should equal(List(
        Map("a" -> nodeA, "b" -> nodeB, "p" -> PathImpl(kernPath: _*))
      ))
    }
  }

  test("decompose_single_varlength_step_introducing_reliterator") {
    //Given:
    //Pattern: a-[r:A*1..2]->b
    //   Path: 0-[:A]->1

    val nodeA = createNode()
    val nodeB = createNode()
    val rel0 = relate(nodeA, nodeB, "A")

    val kernPath = Seq(nodeA, rel0, nodeB)
    val path =
      VariableLengthStepTrail(EndPoint("b"), SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, Some(2), "p", Some("r"), "a", null)

    //Then
    graph.inTx {
      path.decompose(kernPath).toList should equal(List(
        Map("a" -> nodeA, "b" -> nodeB, "p" -> PathImpl(kernPath: _*), "r" -> Seq(rel0))
      ))
    }
  }

  test("decompose_single_step_follow_with_varlength") {
    //Given:
    //Pattern: x-[r1:B]->a-[:A*1..2]->b
    //   Path: 0-[:B]->1-[:A]->2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel1 = relate(node0, node1, "B")
    val rel2 = relate(node1, node2, "A")

    val kernPath = Seq(node0, rel1, node1, rel2, node2)
    val expectedPath = PathImpl(node1, rel2, node2)

    val point = EndPoint("b")
    val lastStep = VariableLengthStepTrail(point, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)
    val firstStep = SingleStepTrail(lastStep, SemanticDirection.OUTGOING, "r1", Seq("B"), "x", True(), True(), null, Seq())

    //Then
    graph.inTx {
      firstStep.decompose(kernPath).toList should equal(List(
        Map("x" -> node0, "a" -> node1, "b" -> node2, "r1" -> rel1, "p" -> expectedPath)
      ))
    }
  }

  test("decompose_varlength_followed_by_single_step") {
    //Given:
    //Pattern: a-[:A*1..2]->b<-[r1:B]-x
    //   Path: 0-[:A     ]->1<-[  :B]-2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel0 = relate(node0, node1, "A")
    val rel1 = relate(node2, node1, "B")

    val kernPath = Seq(node0, rel0, node1, rel1, node2)
    val expectedPath = PathImpl(node0, rel0, node1)
    val bound = EndPoint("x")
    val single = SingleStepTrail(bound, SemanticDirection.INCOMING, "r1", Seq("B"), "b", True(), True(), null, Seq())
    val path = VariableLengthStepTrail(single, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //Then
    graph.inTx {
      path.decompose(kernPath).toList should equal(List(
        Map("x" -> node2, "a" -> node0, "b" -> node1, "r1" -> rel1, "p" -> expectedPath)
      ))
    }
  }

  test("multi_step_variable_length_decompose") {
    //Given:
    //Pattern: a-[:A*1..2]->b<-[:*1..2]-x
    //   Path: 0-[:A]->1-[:A]->2

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val rel0 = relate(node0, node1, "A")
    val rel1 = relate(node1, node2, "A")

    val input = Seq(node0, rel0, node1, rel1, node2)
    val expectedPath = input

    val bound = EndPoint("b")
    val path = VariableLengthStepTrail(bound, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //Then
    graph.inTx {
      path.decompose(input).toList should equal(List(
        Map("a" -> node0, "b" -> node2, "p" -> PathImpl(expectedPath: _*))
      ))
    }
  }

  test("zero_length_trail_can_be_ignored") {
    //Given:
    //Pattern: a-[:A*0..1]->b<-[r1:B]-c
    //   Path: 0<-[:B]-1

    val node0 = createNode()
    val node1 = createNode()
    val rel0 = relate(node0, node1, "B")

    val input = Seq(node0, rel0, node1)
    val expectedPath = PathImpl(node0)

    val bound = EndPoint("c")
    val single = SingleStepTrail(bound, SemanticDirection.INCOMING, "r", Seq("B"), "b", True(), True(), null, Seq())
    val path = VariableLengthStepTrail(single, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 0, Some(1), "p", None, "a", null)

    //Then
    graph.inTx {
      path.decompose(input).toList should equal(List(
        Map("a" -> node0, "b" -> node0, "c" -> node1, "p" -> expectedPath, "r" -> rel0)
      ))
    }
  }

  test("linked_list_using_two_vartrails") {
    //Given:
    //Pattern: a-[:A*0..]->x-[:A*0..]->b
    //   Path: 0-[:A]->1-[:A]->2-[:A]->3-[:A]->4

    val node0 = createNode()
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    val node4 = createNode()

    val rel0 = relate(node0, node1, "A")
    val rel1 = relate(node1, node2, "A")
    val rel2 = relate(node2, node3, "A")
    val rel3 = relate(node3, node4, "A")

    val input = Seq(node0, rel0, node1, rel1, node2, rel2, node3, rel3, node4)

    val bound = EndPoint("b")
    val first = VariableLengthStepTrail(bound, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 0, None, "p2", None, "x", null)
    val second = VariableLengthStepTrail(first, SemanticDirection.OUTGOING, SemanticDirection.OUTGOING, Seq("A"), 0, None, "p1", None, "a", null)

    //Then
    graph.inTx{
      second.decompose(input).toList should equal(List(
        Map("a" -> node0, "x" -> node0, "b" -> node4, "p1" -> PathImpl(node0), "p2" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3, rel3, node4)),
        Map("a" -> node0, "x" -> node1, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1), "p2" -> PathImpl(node1, rel1, node2, rel2, node3, rel3, node4)),
        Map("a" -> node0, "x" -> node2, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2), "p2" -> PathImpl(node2, rel2, node3, rel3, node4)),
        Map("a" -> node0, "x" -> node3, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3), "p2" -> PathImpl(node3, rel3, node4)),
        Map("a" -> node0, "x" -> node4, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3, rel3, node4), "p2" -> PathImpl(node4))
      ))
    }
  }
}
