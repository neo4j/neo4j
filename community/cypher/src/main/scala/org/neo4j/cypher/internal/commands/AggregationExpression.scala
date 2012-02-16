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
package org.neo4j.cypher.internal.commands

import collection.Seq
import org.neo4j.cypher.internal.pipes.aggregation._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

abstract class AggregationExpression extends Expression {
  def apply(m: Map[String, Any]) = if(m.contains(name)) m(name) else null

  override def identifier = Identifier(name, typ)

  def name: String

  def typ: AnyType

  def createAggregationFunction: AggregationFunction
}

case class CountStar() extends AggregationExpression {
  def name = "count(*)"

  def typ = LongType()

  def declareDependencies(extectedType: AnyType) = Seq()

  def rewrite(f: (Expression) => Expression) = f(CountStar())

  def createAggregationFunction = new CountStarFunction

  def exists(f: (Expression) => Boolean) = f(this)

  override def toString() = "count(*)"
}

abstract class AggregationWithInnerExpression(inner:Expression) extends AggregationExpression {
  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(expectedInnerType)
  def expectedInnerType: AnyType
  
  override def identifier = Identifier("%s(%s)".format(name, inner.identifier.name), typ)

  def exists(f: (Expression) => Boolean) = f(this)||inner.exists(f)
}

case class Distinct(innerAggregator: AggregationExpression, expression: Expression) extends AggregationWithInnerExpression(expression) {
  def typ = innerAggregator.identifier.typ

  override def identifier = Identifier("%s(distinct %s)".format(innerAggregator.name, expression.identifier.name), innerAggregator.identifier.typ)

  def expectedInnerType: AnyType = AnyType()

  def name = "distinct"

  def createAggregationFunction = new DistinctFunction(expression, innerAggregator.createAggregationFunction)

  override def declareDependencies(extectedType: AnyType): Seq[Identifier] = innerAggregator.dependencies(extectedType) ++ expression.dependencies(AnyType())

  def rewrite(f: (Expression) => Expression) = innerAggregator.rewrite(f) match {
    case inner: AggregationExpression => f(Distinct(inner, expression.rewrite(f)))
    case _ => throw new ThisShouldNotHappenError("Andrés", "An aggregate expression cannot be rewritten to a non-aggregate expression")
  }
}

case class Count(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = IntegerType()

  def name = "count"

  def createAggregationFunction = new CountFunction(anInner)

  def expectedInnerType: AnyType = AnyType()

  def rewrite(f: (Expression) => Expression) = f(Count(anInner.rewrite(f)))
}

case class Sum(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = NumberType()

  def name = "sum"

  def createAggregationFunction = new SumFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = f(Sum(anInner.rewrite(f)))
}

case class Min(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = NumberType()

  def name = "min"

  def createAggregationFunction = new MinFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = f(Min(anInner.rewrite(f)))
}

case class Max(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = NumberType()

  def name = "max"

  def createAggregationFunction = new MaxFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = f(Max(anInner.rewrite(f)))
}

case class Avg(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = NumberType()

  def name = "avg"

  def createAggregationFunction = new AvgFunction(anInner)

  def expectedInnerType: AnyType = NumberType()

  def rewrite(f: (Expression) => Expression) = f(Avg(anInner.rewrite(f)))
}

case class Collect(anInner: Expression) extends AggregationWithInnerExpression(anInner) {
  def typ = new IterableType(anInner.identifier.typ)

  def name = "collect"

  def createAggregationFunction = new CollectFunction(anInner)

  def expectedInnerType: AnyType = AnyType()

  def rewrite(f: (Expression) => Expression) = f(Collect(anInner.rewrite(f)))
}