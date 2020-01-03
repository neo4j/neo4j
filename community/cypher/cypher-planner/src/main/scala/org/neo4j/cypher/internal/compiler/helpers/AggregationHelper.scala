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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.v4_0.expressions._

import scala.annotation.tailrec

object AggregationHelper {
  private def check[T](aggregationFunction: FunctionInvocation, result: Expression => T, otherResult: T): T = {
    //.head works since min and max (the only functions we care about) always have one argument
    aggregationFunction.args.head match {
      case prop: Property =>
        result(prop)
      case variable: Variable =>
        result(variable)
      case _ =>
        otherResult
    }
  }

  def checkMinOrMax[T](aggregation: Expression, minResult: Expression => T, maxResult: Expression => T, otherResult: T): T = {
    aggregation match {
      case f: FunctionInvocation =>
        f.name match {
          case "min" =>
            check(f, minResult, otherResult)
          case "max" =>
            check(f, maxResult, otherResult)
          case _ => otherResult
        }
      case _ => otherResult
    }
  }

  def extractProperties(aggregationExpressions: Map[String, Expression], renamings: Map[String, Expression]): Set[(String, String)] = {
    aggregationExpressions.values.flatMap {
      extractPropertyForValue(_, renamings).map {
        case Property(Variable(varName), PropertyKeyName(propName)) => (varName, propName)
      }
    }.toSet
  }

  @tailrec
  def extractPropertyForValue(expression: Expression,
                              renamings: Map[String, Expression],
                              property: Option[Property] = None): Option[Property] = {
    expression match {
      case FunctionInvocation(_, _, _, Seq(expr, _*)) =>
        // Cannot handle a function inside an aggregation
        if (expr.isInstanceOf[FunctionInvocation])
          None
        else
          extractPropertyForValue(expr, renamings)
      case prop@Property(Variable(varName), _) =>
        if (renamings.contains(varName))
          extractPropertyForValue(renamings(varName), renamings, Some(prop))
        else
          Some(prop)
      case variable@Variable(varName) =>
        if (renamings.contains(varName) && renamings(varName) != variable)
          extractPropertyForValue(renamings(varName), renamings)
        else if (property.nonEmpty)
          Some(Property(variable, property.get.propertyKey)(property.get.position))
        else
          None
      case _ => None
    }
  }
}
