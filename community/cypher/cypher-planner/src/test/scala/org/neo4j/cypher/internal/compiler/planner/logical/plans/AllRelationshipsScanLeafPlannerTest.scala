/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.plans

import org.mockito.Mockito.when
import org.neo4j.common
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.allRelationshipsScanLeafPlanner
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AllRelationshipsScanLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("simple outgoing directed scan") {
    // given
    val context = planningContext()
    // (a)-[r]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING)

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      DirectedAllRelationshipsScan(varFor("r"), varFor("a"), varFor("b"), Set.empty)
    ))
  }

  test("simple incoming directed scan") {
    // given
    val context = planningContext()
    // (a)<-[r]-(b)
    val qg = pattern(v"r", v"a", v"b", INCOMING)

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      DirectedAllRelationshipsScan(varFor("r"), varFor("b"), varFor("a"), Set.empty)
    ))
  }

  test("simple undirected scan") {
    // given
    val context = planningContext()
    // (a)-[r]-(b)
    val qg = pattern(v"r", v"a", v"b", BOTH)

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      UndirectedAllRelationshipsScan(varFor("r"), varFor("a"), varFor("b"), Set.empty)
    ))
  }

  test("should not scan if pattern has types") {
    // given
    val context = planningContext()
    // (a)-[r:R]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING, "R")

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not scan if variable length pattern") {
    // given
    val context = planningContext()
    // (a)-[:R*]->(b)
    val qg = varPattern(v"r", v"a", v"b", OUTGOING)

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans shouldBe empty
  }

  test("should not plan scan for skipped ids") {
    // given
    val context = planningContext()
    // (a)-[r]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING)

    // then
    allRelationshipsScanLeafPlanner(Set(v"r"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    allRelationshipsScanLeafPlanner(Set(v"a"))(qg, InterestingOrderConfig.empty, context) should be(empty)
    allRelationshipsScanLeafPlanner(Set(v"b"))(qg, InterestingOrderConfig.empty, context) should be(empty)
  }

  test("should not plan scan when rel id is in arguments") {
    // given
    // (a)-[r]->(b)
    val context = planningContext()
    val qg = pattern(v"r", v"a", v"b", OUTGOING)

    // then
    allRelationshipsScanLeafPlanner(Set.empty)(
      qg.withArgumentIds(Set(v"r")),
      InterestingOrderConfig.empty,
      context
    ) should be(empty)
  }

  test("should plan scan if no type index") {
    // given
    val context = planningContext(typeScanEnabled = false)

    // (a)-[r]->(b)
    val qg = pattern(v"r", v"a", v"b", OUTGOING)

    // when
    val resultPlans = allRelationshipsScanLeafPlanner(Set.empty)(qg, InterestingOrderConfig.empty, context)

    // then
    resultPlans should equal(Set(
      DirectedAllRelationshipsScan(varFor("r"), varFor("a"), varFor("b"), Set.empty)
    ))
  }

  private def pattern(
    name: LogicalVariable,
    from: LogicalVariable,
    to: LogicalVariable,
    direction: SemanticDirection,
    types: String*
  ) =
    QueryGraph(
      patternNodes = Set(from, to),
      patternRelationships =
        Set(PatternRelationship(
          name,
          (from, to),
          direction,
          types.map(relTypeName(_)),
          SimplePatternLength
        ))
    )

  private def varPattern(
    name: LogicalVariable,
    from: LogicalVariable,
    to: LogicalVariable,
    direction: SemanticDirection,
    types: String*
  ) =
    QueryGraph(
      patternNodes = Set(from, to),
      patternRelationships =
        Set(PatternRelationship(
          name,
          (from, to),
          direction,
          types.map(relTypeName(_)),
          VarPatternLength(1, None)
        ))
    )

  def planningContext(typeScanEnabled: Boolean = true): LogicalPlanningContext = {
    val planContext = newMockedPlanContext()
    val tokenIndex =
      if (typeScanEnabled) Some(TokenIndexDescriptor(common.EntityType.RELATIONSHIP, IndexOrderCapability.BOTH))
      else None
    when(planContext.relationshipTokenIndex).thenReturn(tokenIndex)
    newMockedLogicalPlanningContext(planContext = planContext, semanticTable = newMockedSemanticTable)
  }

}
