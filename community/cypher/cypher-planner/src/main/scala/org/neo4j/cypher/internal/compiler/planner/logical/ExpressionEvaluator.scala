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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.StringDecimalInteger
import org.neo4j.cypher.internal.expressions.functions.DeterministicFunction.isFunctionDeterministic
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.virtual.MapValue

trait ExpressionEvaluator {

  // Avoid evaluating parameters as it does not work well with query caching.
  // Only allowed temporary to support existing usages.
  def evaluateExpression(expr: Expression, parameters: MapValue): Option[Any]

  private def hasParameters(expr: Expression): Boolean = expr.folder.findAllByClass[Expression].exists {
    case Parameter(_, _, _) => true
    case _                  => false
  }

  final def isDeterministic(expr: Expression): Boolean = {
    expr.folder.findAllByClass[Expression].forall {
      case func: FunctionInvocation => isFunctionDeterministic(func.function)
      // for UDFs we don't know but the result might be non-deterministic
      case _: ResolvedFunctionInvocation => false
      case _                             => true
    }
  }

  /*
   * Returns the evaluated long value from the specified expression if the expression is stable and can be evaluated to a long.
   */
  final def evaluateLongIfStable(expression: Expression): Option[Long] = {
    def isStable(expression: Expression): Boolean = {
      !hasParameters(expression) && isDeterministic(expression)
    }

    expression match {
      case literal: StringDecimalInteger => Some(literal.value)
      case nonLiteral if isStable(nonLiteral) =>
        evaluateExpression(nonLiteral, MapValue.EMPTY)
          .collect { case number: NumberValue => number.longValue() }
      case _ => None
    }
  }
}
