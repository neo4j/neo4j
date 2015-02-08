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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_2.helpers.MapSupport._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

import scala.language.implicitConversions

object Metrics {

  object QueryGraphCardinalityInput {
    def empty = QueryGraphCardinalityInput(Map.empty, Cardinality(1))
  }

  case class QueryGraphCardinalityInput(labelInfo: LabelInfo, inboundCardinality: Cardinality) {
    def withCardinality(c: Cardinality): QueryGraphCardinalityInput =
      copy(inboundCardinality = c)

    def recurse(fromPlan: LogicalPlan)(implicit cardinality: Metrics.CardinalityModel): QueryGraphCardinalityInput = {
      val newCardinalityInput = cardinality(fromPlan, this)
      val newLabels = (labelInfo fuse fromPlan.solved.labelInfo)(_ ++ _)
      copy(labelInfo = newLabels, inboundCardinality = newCardinalityInput)
    }
  }

  // This metric calculates how expensive executing a logical plan is.
  // (e.g. by looking at cardinality, expression selectivity and taking into account the effort
  // required to execute a step)
  type CostModel = (LogicalPlan, QueryGraphCardinalityInput) => Cost

  // This metric estimates how many rows of data a logical plan produces
  // (e.g. by asking the database for statistics)
  type CardinalityModel = (LogicalPlan, QueryGraphCardinalityInput) => Cardinality

  type QueryGraphCardinalityModel = (QueryGraph, QueryGraphCardinalityInput) => Cardinality

  type LabelInfo = Map[IdName, Set[LabelName]]
}

case class Metrics(cost: CostModel,
                   cardinality: CardinalityModel,
                   queryGraphCardinalityModel: QueryGraphCardinalityModel)

case class Cost(gummyBears: Double) extends Ordered[Cost] {

  def +(other: Cost): Cost = other.gummyBears + gummyBears
  def *(other: Multiplier): Cost = gummyBears * other.coefficient
  def +(other: CostPerRow): CostPerRow = other.cost * gummyBears
  //def compare(that: Cost): Double = gummyBears.compare(that.gummyBears)
  def unary_-(): Cost = Cost(-gummyBears)
  override def compare(that: Cost): Int = gummyBears.compare(that.gummyBears)
}

object Cost {
  implicit def lift(amount: Double): Cost = Cost(amount)
  implicit def CostOrdering: Ordering[Cost] = Ordering.by(_.gummyBears.toLong)
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

  def inverse = Multiplier(1.0d / amount)
}

object Cardinality {

  val EMPTY = Cardinality(0)
  val SINGLE = Cardinality(1)

  implicit def lift(amount: Double): Cardinality = Cardinality(amount)

  def min(l: Cardinality, r: Cardinality): Cardinality =
    Cardinality(Math.min(l.amount, r.amount))

  def max(l: Cardinality, r: Cardinality): Cardinality =
    Cardinality(Math.max(l.amount, r.amount))

  object NumericCardinality extends Numeric[Cardinality] {
    def toDouble(x: Cardinality): Double = x.amount
    def toFloat(x: Cardinality): Float = x.amount.toFloat
    def toInt(x: Cardinality): Int = x.amount.toInt
    def toLong(x: Cardinality): Long = x.amount.toLong
    def fromInt(x: Int): Cardinality = Cardinality(x)

    def negate(x: Cardinality): Cardinality = Cardinality(-x.amount)
    def plus(x: Cardinality, y: Cardinality): Cardinality = Cardinality(x.amount + y.amount)
    def times(x: Cardinality, y: Cardinality): Cardinality = Cardinality(x.amount * y.amount)
    def minus(x: Cardinality, y: Cardinality): Cardinality = Cardinality(x.amount - y.amount)
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

case class Selectivity(factor: Double) extends Ordered[Selectivity] {
  def -(other: Selectivity): Selectivity = factor - other.factor
  def *(other: Selectivity): Selectivity = other.factor * factor
  def *(other: Multiplier): Selectivity = factor * other.coefficient
  def ^(a: Int): Selectivity = Math.pow(factor, a)
  def negate: Selectivity = 1 - factor

  def compare(that: Selectivity) = factor.compare(that.factor)
}

object Selectivity {
  def of( value: Double ): Option[Selectivity] = if ( value.isInfinite || value.isNaN ) None else Some(value)

  val ZERO = Selectivity(0.0d)
  val ONE = Selectivity(1.0d)

  implicit def lift(amount: Double): Selectivity = Selectivity(amount)

  implicit def turnSeqIntoSingleSelectivity(p: Seq[Selectivity]):Selectivity =
    p.reduceOption(_ * _).getOrElse(Selectivity(1))
}

trait MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel): CardinalityModel
  def newCostModel(cardinality: CardinalityModel): CostModel
  def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel

  def newMetrics(statistics: GraphStatistics, semanticTable: SemanticTable) = {
    val queryGraphCardinalityModel = newQueryGraphCardinalityModel(statistics, semanticTable)
    val cardinality = newCardinalityEstimator(queryGraphCardinalityModel)
    val cost = newCostModel(cardinality)
    Metrics(cost, cardinality, queryGraphCardinalityModel)
  }
}


