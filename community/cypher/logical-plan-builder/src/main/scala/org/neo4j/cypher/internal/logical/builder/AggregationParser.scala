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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentDesc
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered

object AggregationParser {

  private val aggregationRegex = "(.+?)(?i)(ASC|DESC)?".r

  def unapply(text: String): Option[Expression] = {
    text match {
      case aggregationRegex(Parser(expression), direction) =>
        expression match {
          case f: FunctionInvocation =>
            val order =
              if ("ASC".equalsIgnoreCase(direction)) ArgumentAsc
              else if ("DESC".equalsIgnoreCase(direction)) ArgumentDesc
              else ArgumentUnordered
            Some(f.withOrder(order))
          case e: Expression => Some(e)
          case e             => throw new IllegalArgumentException(s"Unexpected aggregation expression: $e")
        }
      case x => throw new IllegalArgumentException(s"'$x' cannot be parsed as an aggregation expression")
    }
  }
}
