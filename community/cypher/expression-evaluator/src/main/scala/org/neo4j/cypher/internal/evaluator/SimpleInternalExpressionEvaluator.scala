/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator.CONVERTERS
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ParameterMapping
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.createParameterArray
import org.neo4j.cypher.internal.runtime.expressionVariableAllocation
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.internal.kernel.api.IndexReadSession
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

class SimpleInternalExpressionEvaluator extends InternalExpressionEvaluator {

  override def evaluate(expression: String): AnyValue =
    errorContext(expression) {
      val parsedExpression = SimpleInternalExpressionEvaluator.ExpressionParser.parse(expression)
      doEvaluate(parsedExpression, MapValue.EMPTY, CypherRow.empty)
    }

  override def evaluate(
    expression: Expression,
    params: MapValue = MapValue.EMPTY,
    context: CypherRow = CypherRow.empty
  ): AnyValue = errorContext(expression.toString) {
    doEvaluate(expression, params, context)
  }

  def errorContext[T](expr: String)(block: => T): T =
    try block catch {
      case e: Exception =>
        throw new EvaluationException(s"Failed to evaluate expression $expr", e)
    }

  def doEvaluate(expression: Expression, params: MapValue, context: CypherRow): AnyValue = {
    val (expr, paramArray) = withSlottedParams(expression, params)
    val allocated = expressionVariableAllocation.allocate(expr)
    val state = queryState(allocated.nExpressionSlots, paramArray)
    val commandExpr = CONVERTERS.toCommandExpression(Id.INVALID_ID, allocated.rewritten)
    commandExpr(context, state)
  }

  def queryState(nExpressionSlots: Int, slottedParams: Array[AnyValue]) =
    new QueryState(
      query = null,
      resources = null,
      params = slottedParams,
      cursors = null,
      queryIndexes = Array.empty[IndexReadSession],
      nodeLabelTokenReadSession = None,
      relTypeTokenReadSession = None,
      expressionVariables = new Array(nExpressionSlots),
      subscriber = QuerySubscriber.DO_NOTHING_SUBSCRIBER,
      queryMemoryTracker = null,
      memoryTrackerForOperatorProvider = null,
    )

  private def withSlottedParams(input: Expression, params: MapValue): (Expression, Array[AnyValue]) = {
    val mapping: ParameterMapping = input.treeFold(ParameterMapping.empty) {
      case Parameter(name, _) => acc => TraverseChildren(acc.updated(name))
    }

    val rewritten = input.endoRewrite(bottomUp(Rewriter.lift {
      case Parameter(name, typ) => ParameterFromSlot(mapping.offsetFor(name), name, typ)
    }))

    val paramArray = createParameterArray(params, mapping)
    (rewritten, paramArray)
  }
}

object SimpleInternalExpressionEvaluator {
  private val CONVERTERS = new ExpressionConverters(CommunityExpressionConverter(ReadTokenContext.EMPTY, new AnonymousVariableNameGenerator()))

  object ExpressionParser {

    def parse(text: String): Expression = JavaccRule.Expression.apply(text)
  }

}
