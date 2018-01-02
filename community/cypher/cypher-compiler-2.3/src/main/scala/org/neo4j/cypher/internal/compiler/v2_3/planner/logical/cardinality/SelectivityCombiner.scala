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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import java.math

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Selectivity

trait SelectivityCombiner {

  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity]

  // A ∪ B = ¬ ( ¬ A ∩ ¬ B )
  def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity]
}

case object IndependenceCombiner extends SelectivityCombiner {

  override def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] =
    selectivities.reduceOption(_ * _)

  /**
   * We transform the disjunction to a negation of a the conjunction of negations
   * ∪{s ∈ selectivities} = ¬ ∩{ ¬ s | s ∈ selectivities}
   * Where conjunction is computed through multiplication of the factors,
   * and negation is computed as (1 - s.factor).
   * Making the total formula:
   * r = 1 - ∏{s ∈ selectivities}(1 - s.factor)
   * Through expanding this formula we realize an iterative way to compute it:
   * selectivities = {a} ⇒ r1 = 1 - (1-a) = a
   * selectivities = {a, b} ⇒ r2 =  1 - (1-a)(1-b) = a + b - a * b = r1 + b - r1 * b
   * selectivities = {a, b, c} ⇒ r3 =  1 - (1-a)(1-b)(1-c) = a + b + c - a*b - a*c - b*c + a*b*c = r2 + c - r2 * c
   * Making the iterative formula:
   * r[i] = r[i-1] + s[i].factor - r[i-1] * s[i].factor
   * We then implement this formula with reduce.
   */
  override def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] = {
    selectivities.map(_.factor).reduceLeftOption((result, value) => result + value - result * value).flatMap(Selectivity.of)
  }
}

object BigDecimalCombiner {

  def orTogetherBigDecimals(bigDecimals: Seq[math.BigDecimal]): Option[math.BigDecimal] = {
    val inverses = bigDecimals.map(negate)
    andTogetherBigDecimals(inverses).map(negate)
  }

  def andTogetherBigDecimals(bigDecimals: Seq[math.BigDecimal]): Option[math.BigDecimal] = {
    bigDecimals.reduceOption(_ multiply _)
  }

  def negate(bigDecimal: math.BigDecimal): math.BigDecimal = {
    math.BigDecimal.ONE.subtract(bigDecimal)
  }
}
