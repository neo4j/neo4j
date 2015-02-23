/**
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.HardcodedGraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CardinalityCostModel, Cost, StatisticsBackedCardinalityModel, Metrics}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{CardinalityModel, QueryGraphCardinalityInput, QueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class RealLogicalPlanningConfiguration()
  extends LogicalPlanningConfiguration with LogicalPlanningConfigurationAdHocSemanticTable {

  def cardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel, semanticTable: SemanticTable) = {
    val model: Metrics.CardinalityModel = new StatisticsBackedCardinalityModel(queryGraphCardinalityModel)
    ({
      case (plan: LogicalPlan, card: QueryGraphCardinalityInput) => model(plan, card)
    })
  }

  def costModel(cardinality: CardinalityModel): PartialFunction[LogicalPlan, Cost] = {
    val model: Metrics.CostModel = new CardinalityCostModel(cardinality)
    ({
      case (plan: LogicalPlan) => model(plan, QueryGraphCardinalityInput.empty)
    })
  }

  def graphStatistics: GraphStatistics = HardcodedGraphStatistics
  def indexes = Set.empty
  def uniqueIndexes = Set.empty
  def labelCardinality = Map.empty
  def knownLabels = Set.empty

  def qg: QueryGraph = ???
}
