/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
