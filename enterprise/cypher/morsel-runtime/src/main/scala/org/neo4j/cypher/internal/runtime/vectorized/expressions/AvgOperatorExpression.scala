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

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.symbols.CTAny
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.storable.{LongValue, NumberValue, Values}
import org.neo4j.values.utils.ValueMath.overflowSafeAdd
import org.neo4j.values.virtual.{ListValue, VirtualValues}

/*
Vectorized version of the average aggregation function
 */
case class AvgOperatorExpression(anInner: Expression) extends AggregationExpressionOperatorWithInnerExpression(anInner) {

  override def expectedInnerType = CTAny

  override def rewrite(f: (Expression) => Expression): Expression = f(AvgOperatorExpression(anInner.rewrite(f)))

  override def createAggregationMapper: AggregationMapper = new AvgMapper(anInner)

  override def createAggregationReducer: AggregationReducer = new AvgReducer
}

class AvgMapper(value: Expression) extends AggregationMapper {

  private var count: Long = 0L
  private var sum: NumberValue = Values.ZERO_INT

  override def result: AnyValue = VirtualValues.list(longValue(count), sum)

  override def map(data: MorselExecutionContext,
                   state: OldQueryState): Unit = value(data, state) match {
    case Values.NO_VALUE =>
    case number: NumberValue =>
      count += 1
      sum = overflowSafeAdd(sum, number);
  }
}

class AvgReducer extends AggregationReducer {

  private var count: Long = 0L
  private var sum: NumberValue = longValue(0L)

  override def result: AnyValue = if (count > 0L) sum.times(1.0 / count.toDouble) else Values.NO_VALUE

  override def reduce(value: AnyValue): Unit = value match {
    case l: ListValue =>
      count += l.value(0).asInstanceOf[LongValue].longValue()
      sum = overflowSafeAdd(sum, l.value(1).asInstanceOf[NumberValue]);
    case _ =>
  }
}



