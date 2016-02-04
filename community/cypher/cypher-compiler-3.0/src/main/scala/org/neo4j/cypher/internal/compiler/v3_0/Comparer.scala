/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_0.IncomparableValuesException

/**
 * Comparer is a trait that enables it's subclasses to compare to AnyRef with each other.
 */
trait Comparer extends CypherSerializer {

  import Comparer._

  def compare(l: Any, r: Any)(implicit qtx: QueryState): Int = {
    try {
      if ((isString(l) && isString(r)) || (isNumber(l) && isNumber(r)) || (isBoolean(l) && isBoolean(r)))
        CypherOrdering.DEFAULT.compare(l, r)
      else
        throw new IncomparableValuesException(serializeWithType(l), serializeWithType(r))
    } catch {
      case _: IllegalArgumentException =>
        throw new IncomparableValuesException(serializeWithType(l), serializeWithType(r))
    }
  }
}

object Comparer {
  def isString(value: Any): Boolean = value match {
    case _: String => true
    case _: Character => true
    case _ => value == null
  }

  def isNumber(value: Any): Boolean = value match {
    case _: Number => true
    case _ => value == null
  }

  def isBoolean(value: Any): Boolean = value match {
    case _: Boolean => true
    case _ => value == null
  }
}
