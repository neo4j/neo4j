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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Cardinality, Candidates, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

class ApplyOptionalTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val subQueryLookupTable = Map.empty[PatternExpression, QueryGraph]

  test("should introduce apply for unsolved optional match when all arguments are covered") {
    // MATCH (a) OPTIONAL MATCH (a)-[r]->(b)

    val patternRel = newPatternRelationship("a", "b", "r")
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel)
    )
    val qg = QueryGraph(patternNodes = Set("a")).withAddedOptionalMatch(optionalMatch)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: SingleRow => Cardinality(1.0)
      case _            => Cardinality(1000.0)
    })

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext,
      metrics = factory.newMetrics(hardcodedStatistics, newMockedSemanticTable)
    )

    val inputPlan = newMockedQueryPlan("a")
    val planTable = PlanTable(Map(Set(IdName("a")) -> inputPlan))
    val innerPlan = Expand(SingleRow(Set("a"))(), "a", Direction.OUTGOING, Seq.empty, "b", "r", SimplePatternLength)

    val candidates = applyOptional(planTable, qg)

    // Then
    candidates.plans should have size 1
    candidates.plans.head.plan should equal(Apply(inputPlan.plan, Optional(innerPlan)))
  }

  test("should not use apply when optional match is the at the start of the query") {
    // optional match (a) return a

    val optionalMatch = QueryGraph(patternNodes = Set("a"))
    val qg = QueryGraph.empty.withAddedOptionalMatch(optionalMatch)

    implicit val context = newMockedLogicalPlanningContext(
      planContext = newMockedPlanContext
    )

    applyOptional(PlanTable(), qg) should equal(Candidates())
  }
}
