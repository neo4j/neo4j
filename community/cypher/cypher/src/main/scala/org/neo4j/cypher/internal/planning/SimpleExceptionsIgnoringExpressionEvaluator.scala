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
package org.neo4j.cypher.internal.planning

import org.neo4j.cypher.internal.compiler.planner.logical.ExpressionEvaluator
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.CypherException
import org.neo4j.values.virtual.MapValue

/**
 * Wrapper around [[SimpleInternalExpressionEvaluator]] that catches exceptions and returns an Option.
 */
case object SimpleExceptionsIgnoringExpressionEvaluator extends ExpressionEvaluator {
  private val expressionEvaluator = new SimpleInternalExpressionEvaluator()

  override def evaluateExpression(expr: Expression, parameters: MapValue): Option[Any] = {
    try {
      Some(expressionEvaluator.evaluate(expr, parameters))
    } catch {
      case _: CypherException => None // Silently disregard expressions that cannot be evaluated in an empty context
    }
  }
}
