/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyType
import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = IdName("a")
  private val bId = IdName("b")
  private val rId = IdName("r")
  private val argId = IdName("arg")

  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set.empty)(solved)
    val expand = Expand(nodeByLabelScan, aId, SemanticDirection.OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand)(solved)
    val argument = Argument(Set.empty)(solved)(Map.empty)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), None)(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId, bId, rId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a:A)-[r:R]->(b)").plan should equal(emptyResult)
  }

  test("should plan simple expand with argument dependency") {
    val leaf = SingleRow()(solved)
    val projection = Projection(leaf, Map("arg" -> SignedDecimalIntegerLiteral("42")(pos)))(solved)
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set(argId))(solved)
    val selection = Selection(Seq(In(Property(Variable("a")(pos), PropertyKeyName("p")(pos))(pos), Collection(Seq(Variable("arg")(pos)))(pos))(pos)), nodeByLabelScan)(solved)
    val expand = Expand(selection, aId, SemanticDirection.OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand, Set(argId))(solved)
    val argument = Argument(Set(argId))(solved)(Map.empty)
    val createNodeA = MergeCreateNode(argument, aId, Seq(LabelName("A")(pos)), Some(MapExpression(Seq((PropertyKeyName("p")(pos), Variable("arg")(pos))))(pos)))(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId, bId, rId))(solved)
    val apply = Apply(projection, mergeNode)(solved)
    val emptyResult = EmptyResult(apply)(solved)

    planFor("WITH 42 AS arg MERGE (a:A {p: arg})-[r:R]->(b)").plan should equal(emptyResult)
  }

  test("should use AssertSameNode when multiple unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
      uniqueIndexOn("Y", "prop")
    } planFor "MERGE (a:X:Y {prop: 42})-[:T]->(b)").plan

    plan shouldBe using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should not use AssertSameNode when one unique index matches") {
    val plan = (new given {
      uniqueIndexOn("X", "prop")
    } planFor "MERGE (a:X:Y {prop: 42})").plan

    plan should not be using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }
}
