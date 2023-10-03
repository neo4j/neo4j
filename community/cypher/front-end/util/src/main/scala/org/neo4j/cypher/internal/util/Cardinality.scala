/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util

import scala.language.implicitConversions
import scala.util.Try

case class Cardinality(amount: Double) extends Ordered[Cardinality] {

  self =>

  def compare(that: Cardinality): Int = amount.compare(that.amount)
  def *(that: Multiplier): Cardinality = amount * that.coefficient
  def *(that: Selectivity): Cardinality = if (that.factor == 0) Cardinality.EMPTY else amount * that.factor
  def +(that: Cardinality): Cardinality = amount + that.amount
  def -(that: Cardinality): Cardinality = amount - that.amount

  def *(that: Cardinality): Cardinality =
    if (amount == 0 || that.amount == 0) Cardinality.EMPTY
    else Cardinality.noInf(amount * that.amount)
  def /(that: Cardinality): Option[Selectivity] = if (that.amount == 0) None else Selectivity.of(amount / that.amount)
  def *(that: CostPerRow): Cost = amount * that.cost
  def *(that: Cost): Cost = amount * that.gummyBears
  def ^(a: Int): Cardinality = Cardinality.noInf(Math.pow(amount, a))
  def map(f: Double => Double): Cardinality = f(amount)

  def inverse: Multiplier = Multiplier(1.0d / amount)
  def ceil: Cardinality = Math.ceil(amount)
}

object Cardinality {

  val EMPTY: Cardinality = Cardinality(0)
  val SINGLE: Cardinality = Cardinality(1)

  implicit def lift(amount: Double): Cardinality = Cardinality(amount)

  private def noInf(value: Double) = if (value == Double.PositiveInfinity) Double.MaxValue else value

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

    override def parseString(str: String): Option[Cardinality] = {
      Try(Cardinality(str.toDouble)).toOption
    }
  }
}

/**
 * An EffectiveCardinality is the resulting cardinality of a plan when the surrounding plan is taken into account. For instance, when a LIMIT is present, that
 * could in some cases reduce how many rows some children plans need to produce. That will "effectively" reduce the cardinality of those plans.
 * @param amount The cardinality of a plan when the surrounding plan is taken into account.
 * @param originalCardinality The cardinality of a plan without considering the surrounding plan.
 */
case class EffectiveCardinality(amount: Double, originalCardinality: Option[Cardinality] = None)
    extends Ordered[EffectiveCardinality] {
  def compare(that: EffectiveCardinality): Int = amount.compare(that.amount)

  def +(that: EffectiveCardinality): EffectiveCardinality =
    EffectiveCardinality(amount + that.amount, originalCardinality)

  def -(that: EffectiveCardinality): EffectiveCardinality =
    EffectiveCardinality(Math.max(amount - that.amount, 0), originalCardinality)
}

case class Cost(gummyBears: Double) extends Ordered[Cost] {

  def +(other: Cost): Cost = other.gummyBears + gummyBears
  def *(other: Multiplier): Cost = gummyBears * other.coefficient
  def +(other: CostPerRow): CostPerRow = other.cost * gummyBears
  def compare(that: Cost): Int = gummyBears.compare(that.gummyBears)
  def unary_-(): Cost = Cost(-gummyBears)
}

object Cost {
  implicit def lift(amount: Double): Cost = Cost(amount)
  val ZERO: Cost = Cost(0)
}

object CostPerRow {
  implicit def lift(amount: Double): CostPerRow = CostPerRow(amount)
}

case class CostPerRow(cost: Double) {
  def +(other: CostPerRow): CostPerRow = cost + other.cost
  def *(other: Multiplier): CostPerRow = cost * other.coefficient
  def compare(that: CostPerRow): Int = cost.compare(that.cost)
}

case class Multiplier(coefficient: Double) extends Ordered[Multiplier] {
  def +(other: Multiplier): Multiplier = other.coefficient + coefficient
  def -(other: Multiplier): Multiplier = other.coefficient - coefficient
  def *(other: Multiplier): Multiplier = other.coefficient * coefficient
  def *(selectivity: Selectivity): Multiplier = coefficient * selectivity.factor
  def *(cardinality: Cardinality): Cardinality = coefficient * cardinality.amount
  def ^(a: Int): Multiplier = Multiplier(Math.pow(coefficient, a))

  def ceil: Multiplier = Math.ceil(coefficient)

  override def compare(that: Multiplier): Int = coefficient.compareTo(that.coefficient)
}

object Multiplier {

  val ZERO = Multiplier(0.0d)
  val ONE = Multiplier(1.0d)

  implicit def lift(amount: Double): Multiplier = Multiplier(amount)

  def min(l: Multiplier, r: Multiplier): Multiplier =
    Multiplier(Math.min(l.coefficient, r.coefficient))

  def max(l: Multiplier, r: Multiplier): Multiplier =
    Multiplier(Math.max(l.coefficient, r.coefficient))

  def ofDivision(dividend: Cardinality, divisor: Cardinality): Option[Multiplier] =
    if (divisor.amount == 0) None else Multiplier.of(dividend.amount / divisor.amount)

  def of(value: Double): Option[Multiplier] =
    if (value.isInfinite || value.isNaN || value < 0.0) None else Some(Multiplier(value))

  object NumericMultiplier extends Numeric[Multiplier] {
    def toDouble(x: Multiplier): Double = x.coefficient
    def toFloat(x: Multiplier): Float = x.coefficient.toFloat
    def toInt(x: Multiplier): Int = x.coefficient.toInt
    def toLong(x: Multiplier): Long = x.coefficient.toLong
    def fromInt(x: Int): Multiplier = Multiplier(x)

    def negate(x: Multiplier): Multiplier = -x.coefficient
    def plus(x: Multiplier, y: Multiplier): Multiplier = x.coefficient + y.coefficient
    def times(x: Multiplier, y: Multiplier): Multiplier = x.coefficient * y.coefficient
    def minus(x: Multiplier, y: Multiplier): Multiplier = x.coefficient - y.coefficient
    def compare(x: Multiplier, y: Multiplier): Int = x.compare(y)

    override def parseString(str: String): Option[Multiplier] = {
      Try(Multiplier(str.toDouble)).toOption
    }
  }
}

/**
 * Represents a reduction of work due to laziness and limiting
 *
 * @param fraction Expected fraction of the original work that needs to be done
 * @param minimum Expected minimum number of rows to produce
 */
final case class WorkReduction(fraction: Selectivity, minimum: Option[Cardinality] = None) {

  def calculate(original: Cardinality, useMinimum: Boolean = false): Cardinality =
    (useMinimum, minimum) match {
      case (true, Some(card)) => Cardinality.max(original * fraction, card)
      case _                  => original * fraction
    }

  def withFraction(selectivity: Selectivity): WorkReduction = copy(fraction = selectivity)
}

object WorkReduction {
  val NoReduction: WorkReduction = WorkReduction(Selectivity.ONE, None)
}
