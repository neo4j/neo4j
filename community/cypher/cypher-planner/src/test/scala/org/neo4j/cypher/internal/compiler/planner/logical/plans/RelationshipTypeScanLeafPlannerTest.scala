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
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.relationshipTypeScanLeafPlanner
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipTypeScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {
  test("simple outgoing directed type scan") {
    // given
    val context = planningContext()
    //(a)-[:R]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Seq(
      DirectedRelationshipTypeScan("r", "a", relTypeName("R"), "b", Set.empty)
    ))
  }

  test("simple incoming directed type scan") {
    // given
    val context = planningContext()
    //(a)<-[:R]-(b)
    val qg = pattern("r", "a", "b", INCOMING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Seq(
      DirectedRelationshipTypeScan("r", "b", relTypeName("R"), "a", Set.empty)
    ))
  }

  test("simple undirected type scan") {
    // given
    val context = planningContext()
    //(a)-[:R]-(b)
    val qg = pattern("r", "a", "b", BOTH, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Seq(
      UndirectedRelationshipTypeScan("r", "a", relTypeName("R"), "b", Set.empty)
    ))
  }

  test("should not scan if multiple types") {
    // given
    val context = planningContext()
    //(a)-[:R1|R2]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R1", "R2")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not scan if variable length pattern") {
    // given
    val context = planningContext()
    //(a)-[:R*]->(b)
    val qg = varPattern("r", "a", "b", OUTGOING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not plan type scan for skipped ids") {
    // given
    val context = planningContext()
    //(a)-[:R]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // then
    relationshipTypeScanLeafPlanner(Set("r"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    relationshipTypeScanLeafPlanner(Set("a"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    relationshipTypeScanLeafPlanner(Set("b"))(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan type scan when ids are in arguments") {
    // given
    //(a)-[:R]->(b)
    val context = planningContext()
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // then
    relationshipTypeScanLeafPlanner(Set.empty)(qg.withArgumentIds(Set("r")), InterestingOrderConfig.empty, context) should be(empty)
    relationshipTypeScanLeafPlanner(Set.empty)(qg.withArgumentIds(Set("a")), InterestingOrderConfig.empty, context) should be(empty)
    relationshipTypeScanLeafPlanner(Set.empty)(qg.withArgumentIds(Set("b")), InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan type scan if no type index") {
    // given
    val context = planningContext(typeScanEnabled = false)

    //(a)-[:R]->(b)
    val qg = pattern("r", "a", "b", OUTGOING, "R")

    // when
    val resultPlans = relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not plan type scan if hint on start node") {
    // given
    //(a:L)-[:R]->(b)
    val context = planningContext()
    val qg = pattern("r", "a", "b", OUTGOING, "R")
      .addHints(Seq(UsingScanHint(varFor("a"), labelOrRelTypeName("L"))(pos)))

    // then
    relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan type scan if hint on end node") {
    // given
    //(a:L)-[:R]->(b)
    val context = planningContext()
    val qg = pattern("r", "a", "b", OUTGOING, "R")
      .addHints(Seq(UsingScanHint(varFor("b"), labelOrRelTypeName("L"))(pos)))

    // then
    relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan type scan if join hint on node") {
    // given
    //(a:L)-[:R]->(b)
    val context = planningContext()
    val qg = pattern("r", "a", "b", OUTGOING, "R")
      .addHints(Seq(UsingJoinHint(Seq(varFor("b")))(pos)))

    // then
    relationshipTypeScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  private def pattern(name: String, from: String, to: String, direction: SemanticDirection, types: String*) =
    QueryGraph(
      patternNodes = Set(name, from, to),
      patternRelationships = Set(PatternRelationship(name, (from, to), direction, types.map(relTypeName), SimplePatternLength)))

  private def varPattern(name: String, from: String, to: String, direction: SemanticDirection, types: String*) =
    QueryGraph(
      patternNodes = Set(name, from, to),
      patternRelationships = Set(PatternRelationship(name, (from, to), direction, types.map(relTypeName), VarPatternLength(1, None))))

  def planningContext(typeScanEnabled: Boolean = true): LogicalPlanningContext = {
    val planContext = newMockedPlanContext()
    when(planContext.relationshipTypeScanStoreEnabled).thenReturn(typeScanEnabled)
    newMockedLogicalPlanningContext(planContext = planContext, semanticTable = newMockedSemanticTable)
  }

}
