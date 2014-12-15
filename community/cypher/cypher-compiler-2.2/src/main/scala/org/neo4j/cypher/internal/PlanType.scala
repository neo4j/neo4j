/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.PlanType.cantMixProfileAndExplain
import org.neo4j.cypher.internal.compiler.v2_2.InvalidSemanticsException

object PlanType {
  def cantMixProfileAndExplain: Nothing =
    throw new InvalidSemanticsException("Can't mix PROFILE and EXPLAIN")
}

sealed trait PlanType {
  def combineWith(other: PlanType): PlanType
}

case object Normal extends PlanType {
  def combineWith(other: PlanType) = other
}

case object Explained extends PlanType {
  def combineWith(other: PlanType) = other match {
    case Profiled => cantMixProfileAndExplain
    case _        => this
  }
}

case object Profiled extends PlanType {
  def combineWith(other: PlanType) = other match {
    case Explained => cantMixProfileAndExplain
    case _         => this
  }
}

