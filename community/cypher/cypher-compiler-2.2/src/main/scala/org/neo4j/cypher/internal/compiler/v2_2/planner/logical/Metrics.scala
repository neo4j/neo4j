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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable

object Metrics {
  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type CostModel = LogicalPlan => Cost

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for statistics)
  type CardinalityModel = LogicalPlan => Cardinality

  // This metric estimates the selectivity of an expression
  // (e.g. by algebraic analysis or using statistics)
  type SelectivityModel = Expression => Selectivity
}

case class Metrics(cost: CostModel, cardinality: CardinalityModel, selectivity: SelectivityModel)

case class Cost(gummyBears: Double) extends Ordered[Cost] {
  def +(other: Cost): Cost = Cost(other.gummyBears + gummyBears)
  def *(other: Double): Cost = Cost(other * gummyBears)
  def +(other: CostPerRow): CostPerRow = CostPerRow(other.cost * gummyBears)
  def compare(that: Cost): Int = gummyBears.compare(that.gummyBears)
}

case class Cardinality(amount: Double) extends Ordered[Cardinality] {
  def compare(that: Cardinality) = amount.compare(that.amount)
  def *(that: Multiplier) = Cardinality(amount * that.coefficient)
  def *(that: Selectivity) = Cardinality(amount * that.factor)
  def +(that: Cardinality) = Cardinality(amount + that.amount)
  def *(that: Cardinality) = Cardinality(amount * that.amount)
  def /(that: Cardinality) = Selectivity(amount / that.amount)
  def *(that: CostPerRow) = Cost(amount * that.cost)
  def *(that: Cost) = Cost(amount * that.gummyBears)
  def ^(a: Int) = Cardinality(Math.pow(amount, a))
  def map(f: Double => Double) = Cardinality(f(amount))
}

case class CostPerRow(cost: Double) {
  def +(other: CostPerRow) = CostPerRow(cost + other.cost)
}

case class Multiplier(coefficient: Double) {
  def +(other: Multiplier): Multiplier = Multiplier(other.coefficient + coefficient)
  def -(other: Multiplier): Multiplier = Multiplier(other.coefficient - coefficient)
  def *(other: Multiplier): Multiplier = Multiplier(other.coefficient * coefficient)
}

case class Selectivity(factor: Double) extends Ordered[Selectivity] {
  require(factor <= 1, "Selectivity is has an upper limit of 1. Did you intend to use a m")

  def -(other: Selectivity) = Selectivity(factor - other.factor)
  def *(other: Selectivity) = Selectivity(other.factor * factor)
  def *(other: Multiplier) = Selectivity(factor * other.coefficient)
  def ^(a: Int):Selectivity = Selectivity(Math.pow(factor, a))
  def inverse: Selectivity = Selectivity(1 - factor)

  def compare(that: Selectivity) = factor.compare(that.factor)
}

object Selectivity {

  implicit def turnSeqIntoSingleSelectivity(p: Seq[Selectivity]):Selectivity =
    p.reduceOption(_ * _).getOrElse(Selectivity(1))

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


