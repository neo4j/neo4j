/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4


sealed abstract class UpdateStrategy {
  def name: String
  def toTextOutput: String = name
  def alwaysEager: Boolean
}

case object eagerUpdateStrategy extends UpdateStrategy {
  override val name = "EAGER"

  override def alwaysEager = true
}

case object defaultUpdateStrategy extends UpdateStrategy {
  override val name = "DEFAULT"

  override def alwaysEager = false
}

object UpdateStrategy {

  def apply(name: String): UpdateStrategy = name.toUpperCase match {
    case eagerUpdateStrategy.name => eagerUpdateStrategy
    case defaultUpdateStrategy.name => defaultUpdateStrategy

    case n => throw new IllegalArgumentException(
      s"$n is not a valid update strategy, valid options are ${defaultUpdateStrategy.name} and ${eagerUpdateStrategy.name}")
  }
}
