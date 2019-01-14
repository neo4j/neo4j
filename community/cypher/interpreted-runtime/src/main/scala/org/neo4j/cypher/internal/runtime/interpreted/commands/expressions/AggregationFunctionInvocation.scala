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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.UserDefinedAggregator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ValueConversion}
import org.neo4j.cypher.internal.v3_4.logical.plans.UserFunctionSignature
import org.neo4j.values.AnyValue

abstract class AggregationFunctionInvocation(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends AggregationExpression {
  private val valueConverter = ValueConversion.getValueConverter(signature.outputType)

  override def createAggregationFunction: AggregationFunction = new AggregationFunction {
    private var inner: UserDefinedAggregator = _

    override def result(state: QueryState): AnyValue = {
      valueConverter(aggregator(state).result)
    }

    override def apply(data: ExecutionContext, state: QueryState): Unit = {
      val argValues = arguments.map(arg => {
        state.query.asObject(arg(data, state))
      })
      aggregator(state).update(argValues)
    }

    private def aggregator(state: QueryState) = {
      if (inner == null) {
        inner = call(state)
      }
      inner
    }
  }

  override def symbolTableDependencies: Set[String] = arguments.flatMap(_.symbolTableDependencies).toSet

  protected def call(state: QueryState): UserDefinedAggregator
}

case class AggregationFunctionInvocationById(signature: UserFunctionSignature,  arguments: IndexedSeq[Expression])
  extends AggregationFunctionInvocation(signature, arguments)
{
  protected def call(state: QueryState) = {state.query.aggregateFunction(signature.id.get, signature.allowed)}

  override def rewrite(f: (Expression) => Expression): Expression = f(
    AggregationFunctionInvocationById(signature, arguments.map(a => a.rewrite(f))))
}

case class AggregationFunctionInvocationByName(signature: UserFunctionSignature,  arguments: IndexedSeq[Expression])
  extends AggregationFunctionInvocation(signature, arguments)
{
  protected def call(state: QueryState) = {state.query.aggregateFunction(signature.name, signature.allowed)}

  override def rewrite(f: (Expression) => Expression): Expression = f(
    AggregationFunctionInvocationByName(signature, arguments.map(a => a.rewrite(f))))
}
