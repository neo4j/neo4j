/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.commands

import org.neo4j.cypher.symbols._
import collection.Seq
import org.neo4j.cypher.internal.pipes.aggregation._

abstract class AggregationExpression(val functionName: String, inner: Expression) extends Expression {
  def apply(m: Map[String, Any]) = m(identifier.name)

  def createAggregationFunction: AggregationFunction

  def expectedInnerType: AnyType

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(expectedInnerType)
}

case class Distinct(innerAggregator: AggregationExpression, expression: Expression) extends AggregationExpression("distinct", expression) {
  def name: String = "%s(distinct %s)".format(innerAggregator.functionName, expression.identifier.name)

  override def identifier: Identifier = Identifier(name, innerAggregator.identifier.typ)

  def expectedInnerType: AnyType = innerAggregator.expectedInnerType

  def createAggregationFunction = new DistinctFunction(expression, innerAggregator.createAggregationFunction)

  override def declareDependencies(extectedType: AnyType): Seq[Identifier] = {
    expression.dependencies(innerAggregator.expectedInnerType) ++ innerAggregator.dependencies(extectedType)
  }
}

case class Count(anInner: Expression) extends AggregationExpression("count", anInner) {
  def identifier = Identifier("count(" + anInner.identifier.name + ")", IntegerType())

  def createAggregationFunction = new CountFunction(anInner)

  def expectedInnerType: AnyType = AnyType()
}

case class Sum(anInner: Expression) extends AggregationExpression("sum", anInner) {
  def identifier = Identifier("sum(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new SumFunction(anInner)

  def expectedInnerType: AnyType = NumberType()
}

case class Min(anInner: Expression) extends AggregationExpression("min", anInner) {
  def identifier = Identifier("min(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new MinFunction(anInner)

  def expectedInnerType: AnyType = NumberType()
}

case class Max(anInner: Expression) extends AggregationExpression("max", anInner) {
  def identifier = Identifier("max(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new MaxFunction(anInner)

  def expectedInnerType: AnyType = NumberType()
}

case class Avg(anInner: Expression) extends AggregationExpression("avg", anInner) {
  def identifier = Identifier("avg(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new AvgFunction(anInner)

  def expectedInnerType: AnyType = NumberType()
}

case class Collect(anInner: Expression) extends AggregationExpression("collect", anInner) {
  def identifier = Identifier("collect(" + anInner.identifier.name + ")", new IterableType(anInner.identifier.typ))

  def createAggregationFunction = new CollectFunction(anInner)

  def expectedInnerType: AnyType = AnyType()
}
