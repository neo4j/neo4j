/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_3.pipes.LazyType
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2.IdName

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = IdName("a")
  private val bId = IdName("b")
  private val rId = IdName("r")
  private val argId = IdName("arg")

  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set.empty)(solved)
    val expand = Expand(nodeByLabelScan, aId, OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand)(solved)
    val argument = SingleRow()(solved)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), None)(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

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

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

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
          AllNodesScan(IdName("n"), Set())(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("n")))(solved)(),
                  IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("n")))(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set(IdName("n")))(solved)(), Set(IdName("n")))(solved),
                  IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("n")))(solved),
              Seq(IdName("b"), IdName("r")))(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set(IdName("n")))(solved)(),
                IdName("b"), Seq.empty, None)(solved),
              IdName("r"), IdName("n"), LazyType("T"), IdName("b"), None)(solved),
            Seq(IdName("b"), IdName("r")))(solved)
        )(solved)
      )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val plan = planFor("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)")._2
    plan should equal(EmptyResult(
      Apply(
        CartesianProduct(
          AllNodesScan(IdName("n"), Set())(solved),
          AllNodesScan(IdName("m"), Set())(solved)
        )(solved),
        AntiConditionalApply(
          AntiConditionalApply(
            Optional(
              Expand(
                Argument(Set(IdName("n"), IdName("m")))(solved)(),
                IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("m"), IdName("r"), ExpandInto)(solved),
              Set(IdName("n"), IdName("m")))(solved),
            Optional(
              Expand(
                LockNodes(
                  Argument(Set(IdName("n"), IdName("m")))(solved)(),
                  Set(IdName("n"), IdName("m")))(solved),
                IdName("n"), OUTGOING, List(RelTypeName("T")(pos)), IdName("m"), IdName("r"), ExpandInto)(solved),
              Set(IdName("n"), IdName("m")))(solved),
            Vector(IdName("r")))(solved),
          MergeCreateRelationship(
            Argument(Set(IdName("n"), IdName("m")))(solved)(),
            IdName("r"), IdName("n"), LazyType("T"), IdName("m"), None)(solved),
          Vector(IdName("r")))(solved)
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
              AllNodesScan(IdName("n"), Set())(solved),
              AllNodesScan(IdName("m"), Set())(solved)
            )(solved),
            Map("a" -> Variable("n")(pos), "b" -> Variable("m")(pos))
          )(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("a"), IdName("b")))(solved)(),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandInto)(solved),
                Set(IdName("a"), IdName("b"))
              )(solved),
              Optional(
                Expand(
                  LockNodes(
                    Argument(Set(IdName("a"), IdName("b")))(solved)(), Set(IdName("a"), IdName("b")))(solved),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandInto)(solved),
                Set(IdName("a"), IdName("b"))
              )(solved),
              Vector(IdName("r")))(solved),
            MergeCreateRelationship(
              Argument(Set(IdName("a"), IdName("b")))(solved)(),
              IdName("r"), IdName("a"), LazyType("T"), IdName("b"), None)(solved),
            Seq(IdName("r")))(solved)
        )(solved)
      )(solved)
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Apply(
          Projection(
            AllNodesScan(IdName("n"), Set())(solved),
            Map("a" -> Variable("n")(pos))
          )(solved),
          AntiConditionalApply(
            AntiConditionalApply(
              Optional(
                Expand(
                  Argument(Set(IdName("a")))(solved)(),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("a"))
              )(solved),
              Optional(
                Expand(
                  LockNodes(Argument(Set(IdName("a")))(solved)(), Set(IdName("a")))(solved),
                  IdName("a"), OUTGOING, List(RelTypeName("T")(pos)), IdName("b"), IdName("r"), ExpandAll)(solved),
                Set(IdName("a"))
              )(solved),
              Seq(IdName("b"), IdName("r")))(solved),
            MergeCreateRelationship(
              MergeCreateNode(
                Argument(Set(IdName("a")))(solved)(),
                IdName("b"), Seq.empty, None)(solved),
              IdName("r"), IdName("a"), LazyType("T"), IdName("b"), None)(solved),
            Seq(IdName("b"), IdName("r")))(solved)
        )(solved)
      )(solved)
    )
  }
}
