/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.commons.{CypherTestSuite, CypherTestSupport}
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext

trait LogicalPlanningTestSupport extends CypherTestSupport {
  self: CypherTestSuite with MockitoSugar =>

  def newMockedLogicalPlanContext = LogicalPlanContext(
    planContext = self.mock[PlanContext],
    estimator = self.mock[CardinalityEstimator],
    costs = self.mock[CostModel],
    semanticTable = self.mock[SemanticTable],
    queryGraph = self.mock[QueryGraph]
  )

  implicit class RichLogicalPlan(plan: LogicalPlan) {
    def asTableEntry = plan.coveredIds -> plan
  }

  def newMockedLogicalPlan(ids: String*): LogicalPlan = newMockedLogicalPlan(ids.map(IdName).toSet)

  def newMockedLogicalPlan(ids: Set[IdName]): LogicalPlan = {
    val plan = mock[LogicalPlan]
    when(plan.toString).thenReturn(s"MockedLogicalPlan(ids = $ids)")
    when(plan.coveredIds).thenReturn(ids)
    when(plan.solvedPredicates).thenReturn(Seq.empty)
    plan
  }
}
