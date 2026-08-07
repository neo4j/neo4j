/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.util.EffectiveCardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity
import org.scalactic.Equality
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalactic.TripleEquals.convertToEqualizer

object CardinalitySupport {

  implicit object EffectiveCardinalityEquality extends Equality[EffectiveCardinality] {

    def areEqual(a: EffectiveCardinality, b: Any): Boolean = b match {
      case b: EffectiveCardinality => a.amount === (b.amount +- tolerance(a.amount))
      case _                       => false
    }
  }

  implicit object SelectivityEquality extends Equality[Selectivity] {

    def areEqual(a: Selectivity, b: Any): Boolean = b match {
      case b: Selectivity => a.factor === (b.factor +- tolerance(a.factor))
      case _              => false
    }
  }

  implicit object MultiplierEquality extends Equality[Multiplier] {

    def areEqual(a: Multiplier, b: Any): Boolean = b match {
      case b: Multiplier => a.coefficient === (b.coefficient +- tolerance(a.coefficient))
      case _             => false
    }
  }

  /**
   * .00000001% off is acceptable
   * We have to be that strict because otherwise some of the var-expand tests would succeed without considering all path lengths.
   */
  private def tolerance(a: Double) = Math.max(1E-10, a * 1E-10)
}
