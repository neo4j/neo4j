/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.builder

object VariableParser {

  private val raw = "([a-zA-Z0-9]*)".r
  private val escaped = "`(.*)`".r

  def unescaped(varName: String): String = unapply(varName) match {
    case Some(value) => value
    case None => throw new IllegalArgumentException(s"'$varName' cannot be parsed as a variable name")
  }

  def unapply(varName: String): Option[String] = varName match {
    case raw(n) => Some(n)
    case escaped(n) => Some(n)
    case _ => None
  }
}
