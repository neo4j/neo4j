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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.helpers.PropertyAccessHelper.PropertyAccess
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min

import java.util.Locale

import scala.annotation.tailrec

object AggregationHelper {

  private def check[T](aggregationFunction: FunctionInvocation, result: Expression => T, otherResult: T): T = {
    // .head works since min and max (the only functions we care about) always have one argument
    aggregationFunction.args.head match {
      case prop: Property =>
        result(prop)
      case variable: Variable =>
        result(variable)
      case _ =>
        otherResult
    }
  }

  def isOnlyMinOrMaxAggregation(
    groupingExpressions: Map[LogicalVariable, Expression],
    aggregationExpressions: Map[LogicalVariable, Expression]
  ): Boolean = {
    groupingExpressions.isEmpty &&
    aggregationExpressions.size == 1 &&
    aggregationExpressions.values.exists {
      case FunctionInvocation(_, FunctionName(name), _, _) =>
        val nameLower = name.toLowerCase(Locale.ROOT)
        nameLower == Min.name.toLowerCase(Locale.ROOT) || nameLower == Max.name.toLowerCase(Locale.ROOT)
      case _ => false
    }
  }

  def checkMinOrMax[T](
    aggregation: Expression,
    minResult: Expression => T,
    maxResult: Expression => T,
    otherResult: T
  ): T = {
    aggregation match {
      case f: FunctionInvocation =>
        f.name.toLowerCase(Locale.ROOT) match {
          case "min" =>
            check(f, minResult, otherResult)
          case "max" =>
            check(f, maxResult, otherResult)
          case _ => otherResult
        }
      case _ => otherResult
    }
  }

  def extractProperties(
    aggregationExpressions: Map[LogicalVariable, Expression],
    renamings: Map[LogicalVariable, Expression]
  ): Set[PropertyAccess] = {
    aggregationExpressions.values.flatMap {
      extractPropertyForValue(_, renamings).map {
        case Property(v: Variable, PropertyKeyName(propName)) => PropertyAccess(v, propName)
        case _ => throw new IllegalStateException("expression must be a property value")
      }
    }.toSet
  }

  def extractPropertyForValue(expression: Expression, renamings: Map[LogicalVariable, Expression]): Option[Property] = {
    @tailrec
    def inner(
      expression: Expression,
      renamings: Map[LogicalVariable, Expression],
      property: Option[Property]
    ): Option[Property] = {
      expression match {
        case FunctionInvocation(_, _, _, Seq(expr, _*)) =>
          // Cannot handle a function inside an aggregation
          if (expr.isInstanceOf[FunctionInvocation])
            None
          else
            inner(expr, renamings, property)
        case prop @ Property(variable: Variable, _) =>
          if (renamings.contains(variable))
            inner(renamings(variable), renamings, Some(prop))
          else
            Some(prop)
        case variable: Variable =>
          if (renamings.contains(variable) && renamings(variable) != variable)
            inner(renamings(variable), renamings, property)
          else if (property.nonEmpty)
            Some(Property(variable, property.get.propertyKey)(property.get.position))
          else
            None
        case _ => None
      }
    }

    inner(expression, renamings, None)
  }
}
