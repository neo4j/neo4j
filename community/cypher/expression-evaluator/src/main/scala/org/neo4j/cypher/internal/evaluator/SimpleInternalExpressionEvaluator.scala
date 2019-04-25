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
package org.neo4j.cypher.internal.evaluator

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation.Result
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.{ExecutionContext, expressionVariableAllocation}
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.parser.Expressions
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.values.AnyValue
import org.parboiled.scala.{ReportingParseRunner, Rule1}

class SimpleInternalExpressionEvaluator extends InternalExpressionEvaluator {

  import SimpleInternalExpressionEvaluator.CONVERTERS

  override def evaluate(expression: String): AnyValue = {
    try {
      val parsedExpression = SimpleInternalExpressionEvaluator.ExpressionParser.parse(expression)
      evaluate(parsedExpression)
    }
    catch {
      case e: Exception =>
        throw new EvaluationException(s"Failed to evaluate expression $expression", e)
    }
  }

  def evaluate(expr: Expression): AnyValue = {
    val Result(rewritten, nExpressionSlots, _) = expressionVariableAllocation.allocate(expr)
    val commandExpr = CONVERTERS.toCommandExpression(Id.INVALID_ID, rewritten)

    val emptyQueryState = new QueryState(null,
      null,
      Array.empty,
      null,
      Array.empty[IndexReadSession],
      new Array(nExpressionSlots))

    commandExpr(ExecutionContext.empty, emptyQueryState)
  }
}

object SimpleInternalExpressionEvaluator {
  private val CONVERTERS = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))

  object ExpressionParser extends Expressions {
    private val parser: Rule1[Expression] = Expression

    def parse(text: String): Expression = {
      val res = ReportingParseRunner(parser).run(text)
      res.result match {
        case Some(e) => e
        case None => throw new IllegalArgumentException(s"Could not parse expression: ${res.parseErrors}")
      }
    }
  }
}
