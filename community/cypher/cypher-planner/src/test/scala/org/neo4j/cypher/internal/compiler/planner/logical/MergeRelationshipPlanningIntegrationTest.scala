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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.EitherPlan
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LockNodes
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val expand = Expand(nodeByLabelScan, "a", OUTGOING, Seq(RelTypeName("R")(pos)), "b", "r")

    val createNodeA = CreateNode("a", Seq(labelName("A")), None)
    val createNodeB = CreateNode("b", Seq.empty, None)

    val createRel = CreateRelationship("r", "a", RelTypeName("R")(pos), "b", SemanticDirection.OUTGOING, None)

    val mergeNode = Merge(expand, Seq(createNodeA, createNodeB), Seq(createRel), Seq.empty, Seq.empty)
    val emptyResult = EmptyResult(mergeNode)

    planFor("MERGE (a:A)-[r:R]->(b)")._2 should equal(emptyResult)
  }

  test("should plan simple expand with argument dependency") {
    val leaf = Argument()
    val projection = Projection(leaf, Map("arg" -> literalInt(42)))
    val nodeByLabelScan = NodeByLabelScan("a", labelName("A"), Set("arg"), IndexOrderNone)
    val selection = Selection(Seq(equals(prop("a", "p"), varFor("arg"))), nodeByLabelScan)
    val expand = Expand(selection, "a", OUTGOING, Seq(RelTypeName("R")(pos)), "b", "r")

    val createNodeA = CreateNode("a", Seq(labelName("A")), Some(mapOf(("p", varFor("arg")))))
    val createNodeB = CreateNode("b", Seq.empty, None)

    val createRel = CreateRelationship("r", "a", RelTypeName("R")(pos), "b", SemanticDirection.OUTGOING, None)

    val mergeNode = Merge(expand, Seq(createNodeA, createNodeB), Seq(createRel), Seq.empty, Seq.empty)
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
          Merge(
            EitherPlan(
                Expand(
                  Argument(Set("n")),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
                Expand(
                  LockNodes(Argument(Set("n")), Set("n")),
                  "n", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)
            ),
            Seq(CreateNode("b", Seq.empty, None)),
            Seq(CreateRelationship("r", "n", RelTypeName("T")(pos), "b", SemanticDirection.OUTGOING, None)),
            Seq(),
            Seq()
          )
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
        Merge(
          EitherPlan(
            Expand(
              Argument(Set("n", "m")),
              "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto),
            Expand(
              LockNodes(
                Argument(Set("n", "m")),
                Set("n", "m")),
              "n", OUTGOING, List(RelTypeName("T")(pos)), "m", "r", ExpandInto)
          ),
          Seq(),
          Seq(CreateRelationship("r", "n", RelTypeName("T")(pos), "m", SemanticDirection.OUTGOING, None)), Seq(), Seq()
        )
      )
    ))
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
          Merge(
            EitherPlan(
              Expand(
                Argument(Set("a", "b")),
                "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto),
              Expand(
                LockNodes(
                  Argument(Set("a", "b")), Set("a", "b")),
                "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandInto)
            ),
            Seq(),
            Seq(CreateRelationship("r", "a", RelTypeName("T")(pos), "b", SemanticDirection.OUTGOING, None)),
            Seq(),
            Seq()
          )
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
          Merge(
            EitherPlan(
              Expand(
                Argument(Set("a")),
                "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll),
              Expand(
                LockNodes(Argument(Set("a")), Set("a")),
                "a", OUTGOING, List(RelTypeName("T")(pos)), "b", "r", ExpandAll)
            ),
            Seq(CreateNode("b", Seq.empty, None)),
            Seq(CreateRelationship("r", "a", RelTypeName("T")(pos), "b", SemanticDirection.OUTGOING, None)),
            Seq(),
            Seq()
          )
        )
      )
    )
  }
}
