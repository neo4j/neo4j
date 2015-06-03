/*
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import java.math

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Selectivity

trait SelectivityCombiner {

  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity]

  // A ∪ B = ¬ ( ¬ A ∩ ¬ B )
  def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity]
}

case object IndependenceCombiner extends SelectivityCombiner {

  // This is the simple and straight forward way of combining two statistically independent probabilities
  //P(A ∪ B) = P(A) * P(B)
  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] = {
    BigDecimalCombiner.andTogetherBigDecimals(toBigDecimals(selectivities)).map(fromBigDecimal)
  }

  // A ∪ B = ¬ ( ¬ A ∩ ¬ B )
  override def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] = {
    BigDecimalCombiner.orTogetherBigDecimals(toBigDecimals(selectivities)).map(fromBigDecimal)
  }

  private def toBigDecimals(selectivities: Seq[Selectivity]): Seq[math.BigDecimal] = {
    selectivities.map(s => math.BigDecimal.valueOf(s.factor))
  }

  private def fromBigDecimal(bigDecimal: math.BigDecimal): Selectivity = {
    Selectivity(bigDecimal.doubleValue())
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

  private def negate(bigDecimal: math.BigDecimal): math.BigDecimal = {
    math.BigDecimal.ONE.subtract(bigDecimal)
  }
}
