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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.frontend.v2_3.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_3.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.{QueryGraphCardinalityModel, CardinalityModel, CostModel}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{StrictnessMode, IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{PlannerQuery, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.SemanticTable

import scala.language.implicitConversions

object Metrics {

  object QueryGraphSolverInput {
    def empty = QueryGraphSolverInput(Map.empty, Cardinality(1), strictness = None)
  }

  case class QueryGraphSolverInput(labelInfo: LabelInfo, inboundCardinality: Cardinality, strictness: Option[StrictnessMode]) {
    def recurse(fromPlan: LogicalPlan): QueryGraphSolverInput = {
      val newCardinalityInput = fromPlan.solved.estimatedCardinality
      val newLabels = (labelInfo fuse fromPlan.solved.labelInfo)(_ ++ _)
      copy(labelInfo = newLabels, inboundCardinality = newCardinalityInput, strictness = strictness)
    }

    def withPreferredStrictness(strictness: StrictnessMode): QueryGraphSolverInput = copy(strictness = Some(strictness))
  }

  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type CostModel = (LogicalPlan, QueryGraphSolverInput) => Cost

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for statistics)
  type CardinalityModel = (PlannerQuery, QueryGraphSolverInput, SemanticTable) => Cardinality

  type QueryGraphCardinalityModel = (QueryGraph, QueryGraphSolverInput, SemanticTable) => Cardinality

  type LabelInfo = Map[IdName, Set[LabelName]]
}

case class Metrics(cost: CostModel,
                   cardinality: CardinalityModel,
                   queryGraphCardinalityModel: QueryGraphCardinalityModel)

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

  self =>

  def compare(that: Cardinality) = amount.compare(that.amount)
  def *(that: Multiplier): Cardinality = amount * that.coefficient
  def *(that: Selectivity): Cardinality = amount * that.factor
  def +(that: Cardinality): Cardinality = amount + that.amount
  def *(that: Cardinality): Cardinality = amount * that.amount
  def /(that: Cardinality): Option[Selectivity] = if (that.amount == 0) None else Selectivity.of(amount / that.amount)
  def *(that: CostPerRow): Cost = amount * that.cost
  def *(that: Cost): Cost = amount * that.gummyBears
  def ^(a: Int): Cardinality = Math.pow(amount, a)
  def map(f: Double => Double): Cardinality = f(amount)

  def inverse = Multiplier(1.0d / amount)

  def min(other: Cardinality) = if (amount < other.amount) self else other
  def max(other: Cardinality) = if (amount > other.amount) self else other
}

object Cardinality {

  val EMPTY = Cardinality(0)
  val SINGLE = Cardinality(1)

  implicit def lift(amount: Double): Cardinality = Cardinality(amount)

  def min(l: Cardinality, r: Cardinality): Cardinality = Math.min(l.amount, r.amount)

  def max(l: Cardinality, r: Cardinality): Cardinality = Math.max(l.amount, r.amount)

  def sqrt(cardinality: Cardinality): Cardinality = Math.sqrt(cardinality.amount)

  object NumericCardinality extends Numeric[Cardinality] {
    def toDouble(x: Cardinality): Double = x.amount
    def toFloat(x: Cardinality): Float = x.amount.toFloat
    def toInt(x: Cardinality): Int = x.amount.toInt
    def toLong(x: Cardinality): Long = x.amount.toLong
    def fromInt(x: Int): Cardinality = Cardinality(x)

    def negate(x: Cardinality): Cardinality = -x.amount
    def plus(x: Cardinality, y: Cardinality): Cardinality = x.amount + y.amount
    def times(x: Cardinality, y: Cardinality): Cardinality = x.amount * y.amount
    def minus(x: Cardinality, y: Cardinality): Cardinality = x.amount - y.amount
    def compare(x: Cardinality, y: Cardinality): Int = x.compare(y)
  }
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

  val ZERO = Multiplier(0.0d)
  val ONE = Multiplier(1.0d)

  implicit def lift(amount: Double): Multiplier = Multiplier(amount)

  def min(l: Multiplier, r: Multiplier): Multiplier =
    Multiplier(Math.min(l.coefficient, r.coefficient))

  def max(l: Multiplier, r: Multiplier): Multiplier =
    Multiplier(Math.max(l.coefficient, r.coefficient))
}

case class Selectivity private(factor: Double) extends Ordered[Selectivity] {
  assert(factor >= 0 && factor <= 1.0)
  def *(other: Selectivity): Selectivity = Selectivity(other.factor * factor)
  def ^(a: Int): Selectivity = Selectivity(Math.pow(factor, a))
  def negate: Selectivity = {
    val f = 1.0 - factor
    if (factor == 0 || f < 1)
      Selectivity(f)
    else
      Selectivity.CLOSEST_TO_ONE
  }

  def compare(that: Selectivity) = factor.compare(that.factor)
}

object Selectivity {

  def of(value: Double): Option[Selectivity] = if (value.isInfinite || value.isNaN || value < 0.0 || value > 1.0) None else Some(Selectivity(value))

  val ZERO = Selectivity(0.0d)
  val ONE = Selectivity(1.0d)
  val CLOSEST_TO_ONE = Selectivity(1 - 5.56e-17)    // we can get closer, but this is close enough


  implicit def turnSeqIntoSingleSelectivity(p: Seq[Selectivity]): Selectivity =
    p.reduceOption(_ * _).getOrElse(Selectivity(1))
}

trait MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel
  def newCostModel(): CostModel
  def newQueryGraphCardinalityModel(statistics: GraphStatistics): QueryGraphCardinalityModel

  def newMetrics(statistics: GraphStatistics) = {
    val queryGraphCardinalityModel = newQueryGraphCardinalityModel(statistics)
    val cardinality = newCardinalityEstimator(queryGraphCardinalityModel)
    Metrics(newCostModel(), cardinality, queryGraphCardinalityModel)
  }
}


