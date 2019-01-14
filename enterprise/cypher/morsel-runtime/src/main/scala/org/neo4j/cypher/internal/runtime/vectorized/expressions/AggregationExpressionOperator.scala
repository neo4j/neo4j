/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.util.v3_4.SyntaxException
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.values.AnyValue

abstract class AggregationExpressionOperator  extends Expression {

  def apply(ctx: ExecutionContext, state: OldQueryState) =
    throw new UnsupportedOperationException("Aggregations should not be used like this.")

  def createAggregationMapper: AggregationMapper
  def createAggregationReducer: AggregationReducer
}

abstract class AggregationExpressionOperatorWithInnerExpression(inner:Expression) extends AggregationExpressionOperator {
  if(inner.containsAggregate)
    throw new SyntaxException("Can't use aggregate functions inside of aggregate functions.")

  if(! inner.isDeterministic)
    throw new SyntaxException("Can't use non-deterministic (random) functions inside of aggregate functions.")

  def expectedInnerType: CypherType

  def arguments = Seq(inner)

  def symbolTableDependencies: Set[String] = inner.symbolTableDependencies
}

trait AggregationMapper {
  def map(data: MorselExecutionContext, state: OldQueryState): Unit
  def result: AnyValue
}

trait AggregationReducer {
  def reduce(value: AnyValue): Unit
  def result: AnyValue
}
