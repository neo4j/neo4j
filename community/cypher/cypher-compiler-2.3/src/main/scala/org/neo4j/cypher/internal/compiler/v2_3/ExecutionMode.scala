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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.InvalidSemanticsException

object ExecutionMode {
  def cantMixProfileAndExplain: Nothing =
    throw new InvalidSemanticsException("Can't mix PROFILE and EXPLAIN")
}

sealed trait ExecutionMode {
  def combineWith(other: ExecutionMode): ExecutionMode
}

case object NormalMode extends ExecutionMode {
  def combineWith(other: ExecutionMode) = other
}

case object ExplainMode extends ExecutionMode {
  def combineWith(other: ExecutionMode) = other match {
    case ProfileMode => ExecutionMode.cantMixProfileAndExplain
    case _ => this
  }
}

case object ProfileMode extends ExecutionMode {
  def combineWith(other: ExecutionMode) = other match {
    case ExplainMode => ExecutionMode.cantMixProfileAndExplain
    case _ => this
  }
}
