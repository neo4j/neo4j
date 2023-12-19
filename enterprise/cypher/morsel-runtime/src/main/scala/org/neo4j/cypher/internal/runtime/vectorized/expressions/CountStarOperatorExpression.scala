/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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



