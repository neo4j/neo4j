/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.util.v3_4.symbols.CTAny
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.{LongValue, NumberValue, Values}
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualValues.list

/*
Vectorized version of the distinct aggregation function
 */
case class DistinctOperatorExpression(expression: Expression, inner: AggregationExpressionOperator) extends AggregationExpressionOperatorWithInnerExpression(expression) {

  override def expectedInnerType = CTAny

  def rewrite(f: (Expression) => Expression) = inner.rewrite(f) match {
    case inner: AggregationExpressionOperator => f(DistinctOperatorExpression(expression.rewrite(f), inner))
    case _                            => f(DistinctOperatorExpression(expression.rewrite(f), inner))
  }
  override def createAggregationMapper: AggregationMapper = new DistinctMapper(expression, inner.createAggregationMapper)

  override def createAggregationReducer: AggregationReducer = new DistinctReducer(inner.createAggregationReducer)
}

class DistinctMapper(value: Expression, inner: AggregationMapper) extends AggregationMapper {

  private val seen = scala.collection.mutable.Set[AnyValue]()
  private var seenNull = false

  override def result: AnyValue = list(list(seen.toArray:_*), inner.result)

  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit = value(data, state) match {
    case Values.NO_VALUE =>
      if (!seenNull) {
        seenNull = true
        inner.map(data, state)
      }
    case a if !seen(a) =>
      seen += a
      inner.map(data, state)
  }
}

class DistinctReducer(inner: AggregationReducer) extends AggregationReducer {

  private var count: Long = 0L
  private var sum: NumberValue = longValue(0L)

  override def result: AnyValue = if (count > 0L) sum.times(1.0 / count.toDouble) else Values.NO_VALUE

  override def reduce(value: AnyValue): Unit = value match {
    case l: ListValue =>
      count += l.value(0).asInstanceOf[LongValue].longValue()
      sum = sum.plus(l.value(1).asInstanceOf[NumberValue])
    case _ =>
  }
}







