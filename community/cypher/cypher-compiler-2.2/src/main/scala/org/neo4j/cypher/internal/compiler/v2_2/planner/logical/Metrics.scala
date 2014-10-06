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

import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.groupPredicates.EstimatedPredicateCombination
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.{Predicate, combinePredicates}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

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
  type PredicateSelectivityCombiner = Set[EstimatedPredicateCombination] => (Set[Predicate], Selectivity)
}

case class Metrics(cost: CostModel,
                   cardinality: CardinalityModel,
                   selectivity: PredicateSelectivityCombiner,
                   candidateListCreator: Seq[LogicalPlan] => CandidateList)

case class Cost(gummyBears: Double) extends Ordered[Cost] {
  def +(other: Cost): Cost = other.gummyBears + gummyBears
  def *(other: Multiplier): Cost = gummyBears * other.coefficient
  def +(other: CostPerRow): CostPerRow = other.cost * gummyBears
  def compare(that: Cost): Int = gummyBears.compare(that.gummyBears)
  def unary_-(): Cost = Cost(-gummyBears)
}

object Cost {
  implicit def lift(amount: Double): Cost = Cost(amount)
}

case class Cardinality(amount: Double) extends Ordered[Cardinality] {
  def compare(that: Cardinality) = amount.compare(that.amount)
  def *(that: Multiplier): Cardinality = amount * that.coefficient
  def *(that: Selectivity): Cardinality = amount * that.factor
  def +(that: Cardinality): Cardinality = amount + that.amount
  def *(that: Cardinality): Cardinality = amount * that.amount
  def /(that: Cardinality): Selectivity = amount / that.amount
  def *(that: CostPerRow): Cost = amount * that.cost
  def *(that: Cost): Cost = amount * that.gummyBears
  def ^(a: Int): Cardinality = Math.pow(amount, a)
  def map(f: Double => Double): Cardinality = f(amount)
}

object Cardinality {
  implicit def lift(amount: Double): Cardinality = Cardinality(amount)
  implicit def lift(amount: Long): Cardinality = lift(amount.doubleValue())
}

case class CostPerRow(cost: Double) {
  def +(other: CostPerRow): CostPerRow = cost + other.cost
  def *(other: Multiplier): CostPerRow = cost * other.coefficient
}

object CostPerRow {
  implicit def lift(amount: Double): CostPerRow = CostPerRow(amount)
}

case class Multiplier(coefficient: Double) {
  def +(other: Multiplier): Multiplier = other.coefficient + coefficient
  def -(other: Multiplier): Multiplier = other.coefficient - coefficient
  def *(other: Multiplier): Multiplier = other.coefficient * coefficient
}

object Multiplier {
  implicit def lift(amount: Double): Multiplier = Multiplier(amount)
}

case class Selectivity(factor: Double) extends Ordered[Selectivity] {
  def -(other: Selectivity): Selectivity = factor - other.factor
  def *(other: Selectivity): Selectivity = other.factor * factor
  def *(other: Multiplier): Selectivity = factor * other.coefficient
  def ^(a: Int): Selectivity = Math.pow(factor, a)
  def inverse: Selectivity = 1 - factor

  def compare(that: Selectivity) = factor.compare(that.factor)
}

object Selectivity {
  implicit def lift(amount: Double): Selectivity = Selectivity(amount)

  implicit def turnSeqIntoSingleSelectivity(p: Seq[Selectivity]):Selectivity =
    p.reduceOption(_ * _).getOrElse(Selectivity(1))

}

trait MetricsFactory {
  def newCardinalityEstimator(statistics: GraphStatistics, selectivity: PredicateSelectivityCombiner, semanticTable: SemanticTable): CardinalityModel
  def newCostModel(cardinality: CardinalityModel): CostModel
  def newSelectivity(): PredicateSelectivityCombiner
  def newCandidateListCreator(): Seq[LogicalPlan] => CandidateList

  def newMetrics(statistics: GraphStatistics, semanticTable: SemanticTable) = {
    val selectivity = newSelectivity()
    val cardinality = newCardinalityEstimator(statistics, selectivity, semanticTable)
    val cost = newCostModel(cardinality)
    Metrics(cost, cardinality, selectivity, newCandidateListCreator())
  }
}


