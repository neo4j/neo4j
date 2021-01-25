/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.util.Cardinality
import org.scalactic.Equality
import org.scalactic.Tolerance.convertNumericToPlusOrMinusWrapper
import org.scalactic.TripleEquals.convertToEqualizer

object CardinalitySupport {


  implicit object Eq extends Equality[Cardinality] {
    def areEqual(a: Cardinality, b: Any): Boolean = b match {
      case b: Cardinality => a.amount === (b.amount +- tolerance(a))
      case _ => false
    }

    /**
     * .00000001% off is acceptable
     * We have to be that strict because otherwise some of the var-expand tests would succeed without considering all path lengths.
     */
    private def tolerance(a: Cardinality) = Math.max(1e-10, a.amount * 1e-10)
  }
}
