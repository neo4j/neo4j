/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v4_0.expressions.RelTypeName
import org.neo4j.cypher.internal.logical.plans._

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan("a", labelName("A"), Set.empty)
    val expand = Expand(nodeByLabelScan, "a", OUTGOING, Seq(RelTypeName("R")(pos)), "b", "r")

    val optional = Optional(expand)
    val argument = Argument()
    val createNodeA = MergeCreateNode(argument, "a", Seq(labelName("A")), None)
    val createNodeB = MergeCreateNode(createNodeA, "b", Seq.empty, None)

    val onCreate = MergeCreateRelationship(createNodeB, "r", "a", RelTypeName("R")(pos), "b", None)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq("a", "b", "r"))
    val emptyResult = EmptyResult(mergeNode)

    planFor("MERGE (a:A)-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should plan simple expand with argument dependency") {
    val leaf = Argument()
    val projection = Projection(leaf, Map("arg" -> literalInt(42)))
    val nodeByLabelScan = NodeByLabelScan("a", labelName("A"), Set("arg"))
    val selection = Selection(Seq(equals(prop("a", "p"), varFor("arg"))), nodeByLabelScan)
    val expand = Expand(selection, "a", OUTGOING, Seq(RelTypeName("R")(pos)), "b", "r")

    val optional = Optional(expand, Set("arg"))
    val argument = Argument(Set("arg"))
    val createNodeA = MergeCreateNode(argument, "a", Seq(labelName("A")), Some(mapOf(("p", varFor("arg")))))
    val createNodeB = MergeCreateNode(createNodeA, "b", Seq.empty, None)

    val onCreate = MergeCreateRelationship(createNodeB, "r", "a", RelTypeName("R")(pos), "b", None)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq("a", "b", "r"))
    val apply = Apply(projection, mergeNode)
    val emptyResult = EmptyResult(apply)

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
          AllNodesScan("n", Set()),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                  Expand(
                    Argument(Set("n")),
                    "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
                Set("n")),
              Optional(
                  Expand(
                    LockNodes(Argument(Set("n")), Set("n")),
                    "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
                Set("n")),
              Seq("b", "r")),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set("n")),
                "b", Seq.empty, None),
              "r", "n", RelTypeName("T")(pos), "b", None),
            Seq("b", "r"))
        )
      )
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val plan = planFor("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)")._2
    plan should equal(EmptyResult(
      Apply(
        CartesianProduct(
          AllNodesScan("n", Set()),
          AllNodesScan("m", Set())
        ),
        AntiConditionalApply(
          AntiConditionalApply(
            Optional(
                Expand(
                  Argument(Set("n", "m")),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto),
              Set("n", "m")),
            Optional(
                Expand(
                  LockNodes(
                    Argument(Set("n", "m")),
                    Set("n", "m")),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto),
              Set("n", "m")),
            Vector("r")),
          MergeCreateRelationship(
            Argument(Set("n", "m")),
            "r", "n", RelTypeName("T")(pos), "m", None),
          Vector("r"))
      )
    )
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            CartesianProduct(
              AllNodesScan("n", Set()),
              AllNodesScan("m", Set())
            ),
            Map("a" -> varFor("n"), "b" -> varFor("m"))
          ),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                  Expand(
                    Argument(Set("a", "b")),
                    "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto),
                Set("a", "b")
              ),
              Optional(
                  Expand(
                    LockNodes(
                      Argument(Set("a", "b")), Set("a", "b")),
                    "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto),
                Set("a", "b")
              ),
              Vector("r")),
            MergeCreateRelationship(
              Argument(Set("a", "b")),
              "r", "a", RelTypeName("T")(pos), "b", None),
            Seq("r"))
        )
      )
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            AllNodesScan("n", Set()),
            Map("a" -> varFor("n"))
          ),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                  Expand(
                    Argument(Set("a")),
                    "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
                Set("a")
              ),
              Optional(
                  Expand(
                    LockNodes(Argument(Set("a")), Set("a")),
                    "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
                Set("a")
              ),
              Seq("b", "r")),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set("a")),
                "b", Seq.empty, None),
              "r", "a", RelTypeName("T")(pos), "b", None),
            Seq("b", "r"))
        )
      )
    )
  }
}
