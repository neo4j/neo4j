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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_3.logical.plans._

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = "a"
  private val bId = "b"
  private val rId = "r"
  private val argId = "arg"

  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set.empty)(solved)
    val expand = Expand(nodeByLabelScan, aId, OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand)(solved)
    val argument = SingleRow()(solved)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), None)(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, RelTypeName("R")(pos), bId, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId, bId, rId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a:A)-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should plan simple expand with argument dependency") {
    val leaf = SingleRow()(solved)
    val projection = Projection(leaf, Map("arg" -> SignedDecimalIntegerLiteral("42")(pos)))(solved)
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set(argId))(solved)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), PropertyKeyName("p")(pos))(pos), ListLiteral(Seq(Variable("arg")(pos)))(pos))(pos)), nodeByLabelScan)(solved)
    val expand = Expand(selection, aId, OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand, Set(argId))(solved)
    val argument = Argument(Set(argId))(solved)(Map.empty)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), Some(MapExpression(Seq((PropertyKeyName("p")(pos), Variable("arg")(pos))))(pos)))(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, RelTypeName("R")(pos), bId, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId, bId, rId))(solved)
    val apply = Apply(projection, mergeNode)(solved)
    val emptyResult = EmptyResult(apply)(solved)

    planFor("WITH 42 AS arg MERGE (a:A {p: arg})-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should use AssertSameNode when multiple unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
      uniqueIndexOn("Y", "prop")
    } getLogicalPlanFor "MERGE (a:X:Y {prop: 42})-[:T]->(b)")._2

    plan shouldBe using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should not use AssertSameNode when one unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
    } getLogicalPlanFor "MERGE (a:X:Y {prop: 42})")._2

    plan should not be using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    planFor("MATCH (n) MERGE (n)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          AllNodesScan("n", Set())(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set("n"))(solved)(),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)(solved),
                Set("n"))(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set("n"))(solved)(), Set("n"))(solved),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)(solved),
                Set("n"))(solved),
              Seq("b", "r"))(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set("n"))(solved)(),
                "b", Seq.empty, None)(solved),
              "r", "n", RelTypeName("T")(pos), "b", None)(solved),
            Seq("b", "r"))(solved)
        )(solved)
      )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val plan = planFor("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)")._2
    plan should equal(EmptyResult(
      Apply(
        CartesianProduct(
          AllNodesScan("n", Set())(solved),
          AllNodesScan("m", Set())(solved)
        )(solved),
        AntiConditionalApply(
          AntiConditionalApply(
            Optional(
              Expand(
                Argument(Set("n", "m"))(solved)(),
                "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto)(solved),
              Set("n", "m"))(solved),
            Optional(
              Expand(
                LockNodes(
                  Argument(Set("n", "m"))(solved)(),
                  Set("n", "m"))(solved),
                "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto)(solved),
              Set("n", "m"))(solved),
            Vector("r"))(solved),
          MergeCreateRelationship(
            Argument(Set("n", "m"))(solved)(),
            "r", "n", RelTypeName("T")(pos), "m", None)(solved),
          Vector("r"))(solved)
      )(solved)
    )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            CartesianProduct(
              AllNodesScan("n", Set())(solved),
              AllNodesScan("m", Set())(solved)
            )(solved),
            Map("a" -> Variable("n")(pos), "b" -> Variable("m")(pos))
          )(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set("a", "b"))(solved)(),
                  "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto)(solved),
                Set("a", "b")
              )(solved),
              Optional(
                Expand(
                  LockNodes(
                    Argument(Set("a", "b"))(solved)(), Set("a", "b"))(solved),
                  "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto)(solved),
                Set("a", "b")
              )(solved),
              Vector("r"))(solved),
            MergeCreateRelationship(
              Argument(Set("a", "b"))(solved)(),
              "r", "a", RelTypeName("T")(pos), "b", None)(solved),
            Seq("r"))(solved)
        )(solved)
      )(solved)
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            AllNodesScan("n", Set())(solved),
            Map("a" -> Variable("n")(pos))
          )(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set("a"))(solved)(),
                  "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)(solved),
                Set("a")
              )(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set("a"))(solved)(), Set("a"))(solved),
                  "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)(solved),
                Set("a")
              )(solved),
              Seq("b", "r"))(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set("a"))(solved)(),
                "b", Seq.empty, None)(solved),
              "r", "a", RelTypeName("T")(pos), "b", None)(solved),
            Seq("b", "r"))(solved)
        )(solved)
      )(solved)
    )
  }
}
