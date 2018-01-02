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

import org.scalautils.{Tolerance, TripleEquals, Equality}

object CardinalitySupport {

  import TripleEquals._
  import Tolerance._

  implicit object Eq extends Equality[Cardinality] {
    def areEqual(a: Cardinality, b: Any): Boolean = b match {
      case b: Cardinality => a.amount === (b.amount +- tolerance(a))
      case _ => false
    }

    private def tolerance(a: Cardinality) = Math.max(5e-3, a.amount * 5e-3) // .5% off is acceptable
  }
}
