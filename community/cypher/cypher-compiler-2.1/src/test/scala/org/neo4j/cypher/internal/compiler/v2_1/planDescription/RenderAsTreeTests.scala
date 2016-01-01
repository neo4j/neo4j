/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planDescription

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.pipes.{NullPipe, Pipe, PipeMonitor}
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.IntroducedIdentifier

class RenderAsTreeTests extends CypherFunSuite {

  val pipe = NullPipe()(mock[PipeMonitor])

  test("single node is represented nicely") {
    renderAsTree(PlanDescriptionImpl(pipe, "NAME", NoChildren, Seq.empty)) should equal(
      "NAME")
  }

  test("node feeding from other node") {

    val leaf = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq.empty)
    val plan = leaf.andThen(mock[Pipe], "ROOT")

    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +LEAF""".stripMargin)
  }

  test("node feeding from two nodes") {

    val leaf1 = PlanDescriptionImpl(pipe, "LEAF1", NoChildren, Seq.empty)
    val leaf2 = PlanDescriptionImpl(pipe, "LEAF2", NoChildren, Seq.empty)
    val plan = PlanDescriptionImpl(pipe, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty)

    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +LEAF1
        |  |
        |  +LEAF2""".stripMargin)
  }

  test("node feeding of node that is feeding of node") {

    val leaf = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq.empty)
    val intermediate = PlanDescriptionImpl(pipe, "INTERMEDIATE", SingleChild(leaf), Seq.empty)
    val plan = PlanDescriptionImpl(pipe, "ROOT", SingleChild(intermediate), Seq.empty)
    val tree = renderAsTree(plan)

    tree should equal(
      """ROOT
        |  |
        |  +INTERMEDIATE
        |    |
        |    +LEAF""".stripMargin)
  }

  test("root with two auto named nodes") {

    val leaf1 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("a")))
    val leaf2 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("b")))
    val plan = PlanDescriptionImpl(pipe, "ROOT", TwoChildren(leaf1, leaf2), Seq.empty)
    val tree = renderAsTree(plan)

    tree should equal(
      """ROOT
        |  |
        |  +LEAF(0)
        |  |
        |  +LEAF(1)""".stripMargin)
  }

  test("root with two leafs, one of which is deep") {

    val leaf1 = PlanDescriptionImpl(pipe, "LEAF1", NoChildren, Seq.empty)
    val leaf2 = PlanDescriptionImpl(pipe, "LEAF2", NoChildren, Seq.empty)
    val leaf3 = PlanDescriptionImpl(pipe, "LEAF3", NoChildren, Seq.empty)
    val intermediate = PlanDescriptionImpl(pipe, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty)
    val plan = PlanDescriptionImpl(pipe, "ROOT", TwoChildren(intermediate, leaf3), Seq.empty)
    renderAsTree(plan) should equal(
"""ROOT
  |
  +INTERMEDIATE
  |  |
  |  +LEAF1
  |  |
  |  +LEAF2
  |
  +LEAF3""")
  }

  test("root with two intermediate nodes coming from four leaf nodes") {

    val leaf1 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("a")))
    val leaf2 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("b")))
    val leaf3 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("c")))
    val leaf4 = PlanDescriptionImpl(pipe, "LEAF", NoChildren, Seq(IntroducedIdentifier("d")))
    val intermediate1 = PlanDescriptionImpl(pipe, "INTERMEDIATE", TwoChildren(leaf1, leaf2), Seq.empty)
    val intermediate2 = PlanDescriptionImpl(pipe, "INTERMEDIATE", TwoChildren(leaf3, leaf4), Seq.empty)
    val plan = PlanDescriptionImpl(pipe, "ROOT", TwoChildren(intermediate1, intermediate2), Seq.empty)
    renderAsTree(plan) should equal(
      """ROOT
        |  |
        |  +INTERMEDIATE(0)
        |  |  |
        |  |  +LEAF(0)
        |  |  |
        |  |  +LEAF(1)
        |  |
        |  +INTERMEDIATE(1)
        |     |
        |     +LEAF(2)
        |     |
        |     +LEAF(3)""".stripMargin)
  }
}
