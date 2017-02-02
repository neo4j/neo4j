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

import org.neo4j.cypher.internal.frontend.v3_2.InputPosition

sealed case class PrefixNameGenerator(generatorName: String) {
  val prefix = s"  $generatorName"

  def name(position: InputPosition): String = s"$prefix${position.toOffsetString}"

  def isNamed(x: String) = !notNamed(x)
  def notNamed(x: String) = x.startsWith(prefix)
}

object PrefixNameGenerator {
  def namePrefix(prefix: String) = s"  $prefix"
}

object UnNamedNameGenerator extends PrefixNameGenerator("UNNAMED") {
  implicit class NameString(name: String) {
    def isNamed = UnNamedNameGenerator.isNamed(name)
    def unnamed = UnNamedNameGenerator.notNamed(name)
  }
}

object FreshIdNameGenerator extends PrefixNameGenerator("FRESHID")

object AggregationNameGenerator extends PrefixNameGenerator("AGGREGATION")
