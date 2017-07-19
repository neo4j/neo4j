/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.ir.v3_2

case class Cardinality(amount: Double) extends Ordered[Cardinality] {

  self =>

  def compare(that: Cardinality) = amount.compare(that.amount)
  def *(that: Multiplier): Cardinality = amount * that.coefficient
  def *(that: Selectivity): Cardinality = if ( that.factor == 0 ) Cardinality.EMPTY else amount * that.factor
  def +(that: Cardinality): Cardinality = amount + that.amount
  def *(that: Cardinality): Cardinality = if( amount == 0 || that.amount == 0 ) Cardinality.EMPTY
    else Cardinality.noInf(amount * that.amount)
  def /(that: Cardinality): Option[Selectivity] = if (that.amount == 0) None else Selectivity.of(amount / that.amount)
  def *(that: CostPerRow): Cost = amount * that.cost
  def *(that: Cost): Cost = amount * that.gummyBears
  def ^(a: Int): Cardinality = Cardinality.noInf(Math.pow(amount, a))
  def map(f: Double => Double): Cardinality = f(amount)

  def inverse = Multiplier(1.0d / amount)

  def min(other: Cardinality) = if (amount < other.amount) self else other
  def max(other: Cardinality) = if (amount > other.amount) self else other
}

object Cardinality {

  val EMPTY = Cardinality(0)
  val SINGLE = Cardinality(1)

  implicit def lift(amount: Double): Cardinality = Cardinality(amount)

  private def noInf(value: Double) = if( value == Double.PositiveInfinity ) Double.MaxValue else value

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

object CostPerRow {
  implicit def lift(amount: Double): CostPerRow = CostPerRow(amount)
}

case class CostPerRow(cost: Double) {
  def +(other: CostPerRow): CostPerRow = cost + other.cost
  def *(other: Multiplier): CostPerRow = cost * other.coefficient
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
