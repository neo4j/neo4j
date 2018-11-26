/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v4_0.helpers

import org.opencypher.v9_0.expressions.{Expression, FunctionInvocation, Property, Variable}

object AggregationHelper {
  private def fetchFunctionParameter(aggregationFunction: FunctionInvocation): Option[String] = {
    //.head works since min and max (the only functions we care about) always have one argument
    aggregationFunction.args.head match {
      case prop: Property =>
        Some(prop.asCanonicalStringVal)
      case variable: Variable =>
        Some(variable.name)
      case _ =>
        None
    }
  }

  def checkMinOrMax[T](aggregation: Expression, minResult: String => T, maxResult: String => T, otherResult: T): T = {
    aggregation match {
      case f: FunctionInvocation =>
        f.name match {
          case "min" =>
            AggregationHelper.fetchFunctionParameter(f) match {
              case Some(param) => minResult(param)
              case _ => otherResult
            }
          case "max" =>
            AggregationHelper.fetchFunctionParameter(f) match {
              case Some(param) => maxResult(param)
              case _ => otherResult
            }
          case _ => otherResult
        }
      case _ => otherResult
    }
  }
}
