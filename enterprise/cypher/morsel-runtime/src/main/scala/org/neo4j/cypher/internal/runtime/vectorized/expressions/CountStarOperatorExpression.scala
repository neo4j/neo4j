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
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{NumberValue, Values}

/*
Vectorized version of the count star aggregation function
 */
case object CountStarOperatorExpression extends AggregationExpressionOperator {

  override def createAggregationMapper: AggregationMapper = new CountStarMapperAndReducer

  override def createAggregationReducer: AggregationReducer = new CountStarMapperAndReducer

  override def rewrite(f: (Expression) => Expression): Expression = f(CountStarOperatorExpression)

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty
}

private class CountStarMapperAndReducer extends AggregationMapper with AggregationReducer {
  private var count: Long = 0L

  override def result: AnyValue = Values.longValue(count)
  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit =  {
    count += 1L
  }

  override def reduce(value: AnyValue): Unit = value match {
    case l: NumberValue => count += l.longValue()
    case _ =>
  }
}



