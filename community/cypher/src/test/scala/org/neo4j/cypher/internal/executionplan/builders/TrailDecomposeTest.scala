/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.junit.Test
import org.neo4j.graphdb.Direction
import org.scalatest.Assertions
import org.neo4j.cypher.{PathImpl, GraphDatabaseTestBase}
import org.neo4j.cypher.internal.pipes.matching.{VariableLengthStepTrail, SingleStepTrail, EndPoint}

class TrailDecomposeTest extends GraphDatabaseTestBase with Assertions with BuilderTest {
  @Test def decompose_simple_path() {
    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val rel = relate(nodeA, nodeB, "LINK_T")

    val kernPath = Seq(nodeA, rel, nodeB)
    val path = SingleStepTrail(EndPoint("b"), Direction.OUTGOING, "link", Seq("LINK_T"), "a", None, None, null)

    val resultMap = path.decompose(kernPath)
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "link" -> rel)))
  }

  @Test def decompose_little_longer_path() {
    //a-[link2]->b-[link1]->c

    val nodeA = createNode("A")
    val nodeB = createNode("B")
    val nodeC = createNode("C")
    val rel1 = relate(nodeA, nodeB, "LINK_T")
    val rel2 = relate(nodeB, nodeC, "LINK_T")

    val kernPath = Seq(nodeA, rel1, nodeB, rel2, nodeC)

    val aPoint = EndPoint("c")
    val bToA = SingleStepTrail(aPoint, Direction.OUTGOING, "link2", Seq("LINK_T"), "b", None, None, null)
    val cToB = SingleStepTrail(bToA, Direction.OUTGOING, "link1", Seq("LINK_T"), "a", None, None, null)

    val resultMap = cToB.decompose(kernPath)

    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "c" -> nodeC, "link1" -> rel1, "link2" -> rel2)))
  }

  @Test def decompose_single_varlength_step() {
    //Given:
    //Pattern: a-[:A*1..2]->b
    //   Path: 0-[:A]->1

    val nodeA = createNode()
    val nodeB = createNode()
    val rel1 = relate(nodeA, nodeB, "A")

    val kernPath = Seq(nodeA, rel1, nodeB)
    val path =
      VariableLengthStepTrail(EndPoint("b"), Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "p" -> PathImpl(kernPath:_*))))
  }

  @Test def decompose_single_varlength_step_introducing_reliterator() {
    //Given:
    //Pattern: a-[r:A*1..2]->b
    //   Path: 0-[:A]->1

    val nodeA = createNode()
    val nodeB = createNode()
    val rel0 = relate(nodeA, nodeB, "A")

    val kernPath = Seq(nodeA, rel0, nodeB)
    val path =
      VariableLengthStepTrail(EndPoint("b"), Direction.OUTGOING, Seq("A"), 1, Some(2), "p", Some("r"), "a", null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("a" -> nodeA, "b" -> nodeB, "p" -> PathImpl(kernPath:_*), "r"->Seq(rel0))))
  }

  @Test def decompose_single_step_follow_with_varlength() {
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
    val lastStep = VariableLengthStepTrail(point, Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)
    val firstStep = SingleStepTrail(lastStep, Direction.OUTGOING, "r1", Seq("B"), "x", None, None, null)

    //When
    val resultMap = firstStep.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("x" -> node0, "a" -> node1, "b" -> node2, "r1" -> rel1, "p" -> expectedPath)))
  }

  @Test def decompose_varlength_followed_by_single_step() {
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
    val single = SingleStepTrail(bound, Direction.INCOMING, "r1", Seq("B"), "b", None, None, null)
    val path = VariableLengthStepTrail(single, Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(kernPath)

    //Then
    assert(resultMap === List(Map("x" -> node2, "a" -> node0, "b" -> node1, "r1" -> rel1, "p" -> expectedPath)))
  }

  @Test def multi_step_variable_length_decompose() {
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
    val path = VariableLengthStepTrail(bound, Direction.OUTGOING, Seq("A"), 1, Some(2), "p", None, "a", null)

    //When
    val resultMap = path.decompose(input)

    //Then
    assert(resultMap === List(Map("a" -> node0, "b" -> node2, "p" -> PathImpl(expectedPath:_*))))
  }

  @Test def zero_length_trail_can_be_ignored() {
    //Given:
    //Pattern: a-[:A*0..1]->b<-[r1:B]-c
    //   Path: 0<-[:B]-1

    val node0 = createNode()
    val node1 = createNode()
    val rel0 = relate(node0, node1, "B")

    val input = Seq(node0, rel0, node1)
    val expectedPath = PathImpl(node0)

    val bound = EndPoint("c")
    val single = SingleStepTrail(bound, Direction.INCOMING, "r", Seq("B"), "b", None, None, null)
    val path = VariableLengthStepTrail(single, Direction.OUTGOING, Seq("A"), 0, Some(1), "p", None, "a", null)

    //When
    val resultMap = path.decompose(input)

    //Then
    assert(resultMap === List(Map("a" -> node0, "b" -> node0, "c" -> node1, "p" -> expectedPath, "r" -> rel0)))
  }

  @Test def linked_list_using_two_vartrails() {
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
    val first = VariableLengthStepTrail(bound, Direction.OUTGOING, Seq("A"), 0, None, "p2", None, "x", null)
    val second = VariableLengthStepTrail(first, Direction.OUTGOING, Seq("A"), 0, None, "p1", None, "a", null)

    //When
    val resultMap = second.decompose(input)

    //Then
    assert(resultMap === List(
      Map("a" -> node0, "x" -> node0, "b" -> node4, "p1" -> PathImpl(node0), "p2" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3, rel3, node4)),
      Map("a" -> node0, "x" -> node1, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1), "p2" -> PathImpl(node1, rel1, node2, rel2, node3, rel3, node4)),
      Map("a" -> node0, "x" -> node2, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2), "p2" -> PathImpl(node2, rel2, node3, rel3, node4)),
      Map("a" -> node0, "x" -> node3, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3), "p2" -> PathImpl(node3, rel3, node4)),
      Map("a" -> node0, "x" -> node4, "b" -> node4, "p1" -> PathImpl(node0, rel0, node1, rel1, node2, rel2, node3, rel3, node4), "p2" -> PathImpl(node4))
    ))
  }
}