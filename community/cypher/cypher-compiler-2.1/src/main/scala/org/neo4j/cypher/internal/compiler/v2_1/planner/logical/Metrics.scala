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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan
import Metrics._
import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable

object Metrics {
  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type CostModel = LogicalPlan => Cost

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for statistics)
  type CardinalityModel = LogicalPlan => Double

  // This metric estimates the selectivity of an expression
  // (e.g. by algebraic analysis or using statistics)
  type SelectivityModel = Expression => Double
}

case class Metrics(cost: CostModel, cardinality: CardinalityModel, selectivity: SelectivityModel)
case class Cost(gummyBears: Double) extends Ordered[Cost] {
  def +(other:Double): Double = other + gummyBears
  def +(other:Cost): Cost = Cost(other.gummyBears + gummyBears)
  def *(other:Double): Double = other * gummyBears

  def compare(that: Cost): Int = gummyBears.compare(that.gummyBears)
}

trait MetricsFactory {
  def newSelectivityEstimator(statistics: GraphStatistics, semanticTable: SemanticTable): SelectivityModel
  def newCardinalityEstimator(statistics: GraphStatistics, selectivity: SelectivityModel, semanticTable: SemanticTable): CardinalityModel
  def newCostModel(cardinality: CardinalityModel): CostModel

  def newMetrics(statistics: GraphStatistics, semanticTable: SemanticTable) = {
    val selectivity = newSelectivityEstimator(statistics, semanticTable)
    val cardinality = newCardinalityEstimator(statistics, selectivity, semanticTable)
    val cost = newCostModel(cardinality)
    Metrics(cost, cardinality, selectivity)
  }
}


