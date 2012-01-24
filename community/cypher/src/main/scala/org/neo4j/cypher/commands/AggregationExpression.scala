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

abstract class AggregationExpression(inner: Expression) extends Expression {
  def apply(m: Map[String, Any]) = m(identifier.name)

  override def identifier = Identifier(name, typ)

  def name: String

  def typ: AnyType

  def createAggregationFunction: AggregationFunction

  def expectedInnerType: AnyType

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(expectedInnerType)
}

case class Distinct(innerAggregator: AggregationExpression, expression: Expression, name: String) extends AggregationExpression(expression) {
  def typ = innerAggregator.identifier.typ

  def expectedInnerType: AnyType = innerAggregator.expectedInnerType

  def createAggregationFunction = new DistinctFunction(expression, innerAggregator.createAggregationFunction)

  override def declareDependencies(extectedType: AnyType): Seq[Identifier] = {
    expression.dependencies(innerAggregator.expectedInnerType) ++ innerAggregator.dependencies(extectedType)
  }

  def rewrite(f: (Expression) => Expression) = Distinct(innerAggregator, f(expression.rewrite(f)), name)
}

case class Count(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = IntegerType()

  def createAggregationFunction = new CountFunction(anInner)

  def expectedInnerType: AnyType = AnyType()

  def rewrite(f: (Expression) => Expression) = Count(f(anInner.rewrite(f)), name)
}

case class Sum(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = NumberType()

  def createAggregationFunction = new SumFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = Sum(f(anInner.rewrite(f)), name)
}

case class Min(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = NumberType()

  def createAggregationFunction = new MinFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = Min(f(anInner.rewrite(f)), name)
}

case class Max(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = NumberType()

  def createAggregationFunction = new MaxFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = Max(f(anInner.rewrite(f)), name)
}

case class Avg(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = NumberType()

  def createAggregationFunction = new AvgFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = Avg(f(anInner.rewrite(f)), name)
}

case class Collect(anInner: Expression, name:String) extends AggregationExpression(anInner) {
  def typ = new IterableType(anInner.identifier.typ)

  def createAggregationFunction = new CollectFunction(anInner)

  def expectedInnerType: AnyType = AnyType()

  def rewrite(f: (Expression) => Expression) = Collect(f(anInner.rewrite(f)), name)
}