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
package org.neo4j.cypher.internal.runtime.vectorized.operators

;

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.expressions.{AggregationExpressionOperator, AggregationMapper, AggregationReducer}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{LongArray, Values}

import scala.collection.mutable

//Dummy aggregation, for test only
case class DummyEvenNodeIdAggregation(offset: Int) extends AggregationExpressionOperator {

  override def createAggregationMapper: AggregationMapper = new EvenNodeIdMapper(offset)

  override def createAggregationReducer: AggregationReducer = new EvenNodeIdReducer

  override def rewrite(f: (Expression) => Expression): Expression = ???

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty
}

class DummyExpression(values: AnyValue*) extends Expression {
  private var current = 0

  override def rewrite(f: (Expression) => Expression): Expression = this

  override def arguments: Seq[Expression] = Seq.empty

  override def symbolTableDependencies: Set[String] = Set.empty

  override def apply(ctx: ExecutionContext,
                     state: OldQueryState): AnyValue = {
    val next = values(current)
    current = (current + 1) % values.length
    next
  }
}

private class EvenNodeIdMapper(offset: Int) extends AggregationMapper {

  private val evenNodes = mutable.Set[Long]()

  override def map(data: MorselExecutionContext, ignore: OldQueryState): Unit = {
    val id = data.getLongAt(offset)
    if (id % 2 == 0) evenNodes.add(id)
  }

  override def result: AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

private class EvenNodeIdReducer extends AggregationReducer {

  private val evenNodes = mutable.Set[Long]()

  override def reduce(value: AnyValue): Unit = value match {
    case ls: LongArray => ls.asObjectCopy().foreach(evenNodes.add)
  }

  override def result: AnyValue = Values.longArray(evenNodes.toArray.sorted)
}

