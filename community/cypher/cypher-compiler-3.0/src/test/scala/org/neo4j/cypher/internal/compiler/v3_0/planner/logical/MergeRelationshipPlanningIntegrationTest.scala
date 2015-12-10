/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_0.pipes.{LazyType, LazyLabel}
import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast.{LabelName, RelTypeName, Collection, In, MapExpression, Property, PropertyKeyName, SignedDecimalIntegerLiteral, Variable}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private val aId = IdName("a")
  private val bId = IdName("b")
  private val rId = IdName("r")

  test("should plan simple expand") {
    val nodeByLabelScan = NodeByLabelScan(aId, LabelName("A")(pos), Set.empty)(solved)
    val expand = Expand(nodeByLabelScan, aId, SemanticDirection.OUTGOING, Seq(RelTypeName("R")(pos)), bId, rId)(solved)

    val optional = Optional(expand)(solved)
    val createNodeA = MergeCreateNode(SingleRow()(solved), aId, Seq(LabelName("A")(pos)), None)(solved)
    val createNodeB = MergeCreateNode(createNodeA, bId, Seq.empty, None)(solved)

    val onCreate = MergeCreateRelationship(createNodeB, rId, aId, LazyType("R"), bId, None)(solved)

    val mergeNode = AntiConditionalApply(optional, onCreate, Seq(aId, bId, rId))(solved)
    val emptyResult = EmptyResult(mergeNode)(solved)

    planFor("MERGE (a:A)-[r:R]->(b)").plan should equal(emptyResult)
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
