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
import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}

/*
Vectorized version of the min and max aggregation functions
 */
abstract class MinOrMaxOperatorExpression(expression: Expression)
  extends AggregationExpressionOperatorWithInnerExpression(expression) {

  override def expectedInnerType = CTAny
}

case class MinOperatorExpression(expression: Expression) extends MinOrMaxOperatorExpression(expression) {
  override def createAggregationMapper: AggregationMapper = new MinMapper(expression)
  override def createAggregationReducer: AggregationReducer = new MinReducer
  override def rewrite(f: (Expression) => Expression): Expression = f(MinOperatorExpression(expression.rewrite(f)))

}

case class MaxOperatorExpression(expression: Expression) extends MinOrMaxOperatorExpression(expression) {
  override def createAggregationMapper: AggregationMapper = new MaxMapper(expression)
  override def createAggregationReducer: AggregationReducer = new MaxReducer
  override def rewrite(f: (Expression) => Expression): Expression = f(MaxOperatorExpression(expression.rewrite(f)))
}

trait MinMaxChecker {
  protected var optimum: AnyValue = Values.NO_VALUE

  def keep(comparisonResult: Int): Boolean

  def result: AnyValue = optimum

  protected def checkIfLargest(value: AnyValue) {
    if (optimum == Values.NO_VALUE) {
      optimum = value
    } else if (keep(AnyValues.COMPARATOR.compare(optimum, value))) {
      optimum = value
    }
  }


   def reduce(value: AnyValue): Unit = value match {
    case Values.NO_VALUE =>
    case value: AnyValue=> checkIfLargest(value)
  }
}

trait MinChecker extends MinMaxChecker {
  override def keep(comparisonResult: Int): Boolean = comparisonResult > 0

}

trait MaxChecker extends MinMaxChecker {
  override def keep(comparisonResult: Int): Boolean = comparisonResult < 0
}

class MinMapper(value: Expression) extends AggregationMapper with MinChecker {

  def map(data: MorselExecutionContext,
          state: OldQueryState): Unit = reduce(value(data, state))
}

class MinReducer extends AggregationReducer with MinChecker

class MaxMapper(value: Expression) extends AggregationMapper with MaxChecker {

  def map(data: MorselExecutionContext,
          state: OldQueryState): Unit = reduce(value(data, state))
}

class MaxReducer extends AggregationReducer with MaxChecker








