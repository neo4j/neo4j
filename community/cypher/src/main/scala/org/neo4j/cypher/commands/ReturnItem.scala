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

import org.neo4j.cypher.internal.pipes.Dependant
import collection.Seq
import org.neo4j.cypher.symbols.{AnyType, IntegerType, Identifier}
import org.neo4j.cypher.internal.pipes.aggregation._

abstract sealed class ReturnItem(val identifier: Identifier) extends (Map[String, Any] => Any) with Dependant {
  def columnName = identifier.name

  def concreteReturnItem = this

  override def toString() = identifier.name
}

object ExpressionReturnItem {
  def apply(value: Expression) = new ExpressionReturnItem(value, value.identifier.name)
}


case class ExpressionReturnItem(value: Expression, name: String) extends ReturnItem(Identifier(name, value.identifier.typ)  ) {
  def apply(m: Map[String, Any]): Any = m.get(value.identifier.name) match {
    case None => value(m)
    case Some(x) => x
  }

  def dependencies: Seq[Identifier] = value.dependencies(AnyType())
}

case class AliasReturnItem(inner: ReturnItem, newName: String) extends ReturnItem(Identifier(newName, inner.identifier.typ)) {
  def apply(m: Map[String, Any]): Any = inner.apply(m)

  def dependencies: Seq[Identifier] = inner.dependencies

  override def toString() = inner.toString() + " AS " + newName
}

abstract sealed class AggregationItem(typ: AnyType, name: String) extends ReturnItem(Identifier(name, typ)) {
  def apply(m: Map[String, Any]): Map[String, Any] = m

  def createAggregationFunction: AggregationFunction

  override def toString() = identifier.name
}

object ValueAggregationItem {
  def apply(value: AggregationExpression) = new ValueAggregationItem(value, value.identifier.name)
}

case class ValueAggregationItem(value: AggregationExpression, name:String) extends AggregationItem(value.identifier.typ, name) {
  def dependencies: Seq[Identifier] = value.dependencies(AnyType())

  def createAggregationFunction = value.createAggregationFunction
}

case class AliasAggregationItem(inner: AggregationItem, newName: String) extends AggregationItem(inner.identifier.typ, newName) {
  def createAggregationFunction = inner.createAggregationFunction

  def dependencies: Seq[Identifier] = inner.dependencies
}

object CountStar {
  def apply() = new CountStar("count(*)")
}

case class CountStar(name:String) extends AggregationItem(IntegerType(), name) {
  def createAggregationFunction = new CountStarFunction

  def dependencies: Seq[Identifier] = Seq()
}