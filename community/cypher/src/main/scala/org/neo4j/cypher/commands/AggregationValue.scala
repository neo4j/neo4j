package org.neo4j.cypher.commands

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.cypher.pipes.aggregation._
import org.neo4j.cypher.symbols._
import collection.Seq

abstract class AggregationValue(val functionName: String, inner: Value) extends Value {
  def apply(m: Map[String, Any]) = m(identifier.name)

  def checkAvailable(symbols: SymbolTable) {
    inner.checkAvailable(symbols)
  }

  def createAggregationFunction: AggregationFunction

  def dependsOn: Set[String] = inner.dependsOn

  def dependencies: Seq[Identifier] = inner.dependencies
}

case class Distinct(innerAggregator: AggregationValue, innerValue: Value) extends AggregationValue("distinct", innerValue) {
  override def identifier: Identifier = Identifier("%s(distinct %s)".format(innerAggregator.functionName, innerValue.identifier.name), innerAggregator.identifier.typ)

  def createAggregationFunction: AggregationFunction = new DistinctFunction(innerValue, innerAggregator.createAggregationFunction)

  override def dependencies: Seq[Identifier] = innerAggregator.dependencies ++ innerValue.dependencies
}

case class Count(anInner: Value) extends AggregationValue("count", anInner) {
  def identifier = Identifier("count(" + anInner.identifier.name + ")", IntegerType())

  def createAggregationFunction = new CountFunction(anInner)
}

case class Sum(anInner: Value) extends AggregationValue("sum", anInner) {
  def identifier = Identifier("sum(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new SumFunction(anInner)
}

case class Min(anInner: Value) extends AggregationValue("min", anInner) {
  def identifier = Identifier("min(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new MinFunction(anInner)
}

case class Max(anInner: Value) extends AggregationValue("max", anInner) {
  def identifier = Identifier("max(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new MaxFunction(anInner)
}

case class Avg(anInner: Value) extends AggregationValue("avg", anInner) {
  def identifier = Identifier("avg(" + anInner.identifier.name + ")", NumberType())

  def createAggregationFunction = new AvgFunction(anInner)
}

case class Collect(anInner: Value) extends AggregationValue("collect", anInner) {
  def identifier = Identifier("collect(" + anInner.identifier.name + ")", new IterableType(anInner.identifier.typ))

  def createAggregationFunction = new CollectFunction(anInner)
}



































