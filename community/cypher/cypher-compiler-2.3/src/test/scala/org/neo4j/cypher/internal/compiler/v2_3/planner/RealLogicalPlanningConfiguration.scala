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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.compiler.v2_3.HardcodedGraphStatistics
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.{QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CardinalityCostModel, Cost, Metrics, StatisticsBackedCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.SemanticTable

case class RealLogicalPlanningConfiguration()
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  override def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel) = {
    val model: Metrics.CardinalityModel = new StatisticsBackedCardinalityModel(queryGraphCardinalityModel)
    ({
      case (pq: PlannerQuery, card: QueryGraphSolverInput, semanticTable: SemanticTable) => model(pq, card, semanticTable)
    })
  }

  override def costModel(): PartialFunction[(LogicalPlan, QueryGraphSolverInput), Cost] = {
    val model: Metrics.CostModel = CardinalityCostModel
    ({
      case (plan: LogicalPlan, input: QueryGraphSolverInput) => model(plan, input)
    })
  }

  override def graphStatistics: GraphStatistics = HardcodedGraphStatistics
  override def indexes = Set.empty
  override def uniqueIndexes = Set.empty
  override def labelCardinality = Map.empty
  override def knownLabels = Set.empty

  override def qg: QueryGraph = ???
}
