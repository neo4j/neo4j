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
package org.neo4j.cypher.commands

import org.neo4j.cypher.pipes.aggregation._
import scala.collection.JavaConverters._
import org.neo4j.graphdb._
import org.neo4j.cypher.{ParameterNotFoundException, SyntaxException, SymbolTable}

abstract sealed class Value extends (Map[String, Any] => Any) {
  def identifier: Identifier

  def checkAvailable(symbols: SymbolTable)
}

case class Literal(v: Any) extends Value {
  def apply(m: Map[String, Any]) = v

  def identifier: Identifier = LiteralIdentifier(v.toString)

  def checkAvailable(symbols: SymbolTable) {}
}

abstract case class FunctionValue(functionName: String, arguments: Value*) extends Value {

  def identifier: Identifier = ValueIdentifier(functionName + "(" + arguments.map(_.identifier.name).mkString(",") + ")");

  def checkAvailable(symbols: SymbolTable) {
    arguments.foreach(_.checkAvailable(symbols))
  }
}


abstract class AggregationValue(functionName: String, inner: Value) extends Value {
  def apply(m: Map[String, Any]) = m(identifier.name)

  def identifier: Identifier = AggregationIdentifier(functionName + "(" + inner.identifier.name + ")")

  def checkAvailable(symbols: SymbolTable) {
    inner.checkAvailable(symbols)
  }

  def createAggregationFunction: AggregationFunction
}

case class Count(anInner: Value) extends AggregationValue("count", anInner) {
  def createAggregationFunction = new CountFunction(anInner)
}

case class Sum(anInner: Value) extends AggregationValue("sum", anInner) {
  def createAggregationFunction = new SumFunction(anInner)
}

case class Min(anInner: Value) extends AggregationValue("min", anInner) {
  def createAggregationFunction = new MinFunction(anInner)
}

case class Max(anInner: Value) extends AggregationValue("max", anInner) {
  def createAggregationFunction = new MaxFunction(anInner)
}

case class Avg(anInner: Value) extends AggregationValue("avg", anInner) {
  def createAggregationFunction = new AvgFunction(anInner)
}

case class NullablePropertyValue(subEntity: String, subProperty: String) extends PropertyValue(subEntity, subProperty) {
  protected override def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = null
}

case class PropertyValue(entity: String, property: String) extends Value {
  protected def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = throw new SyntaxException("%s.%s does not exist on %s".format(entity, property, propertyContainer), x)

  def apply(m: Map[String, Any]): Any = {
    val propertyContainer = m(entity).asInstanceOf[PropertyContainer]
    try {
      propertyContainer.getProperty(property)
    } catch {
      case x: NotFoundException => handleNotFound(propertyContainer, x)
    }
  }

  def identifier: Identifier = PropertyIdentifier(entity, property)

  def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(PropertyContainerIdentifier(entity))
  }
}

case class RelationshipTypeValue(relationship: Value) extends FunctionValue("TYPE", relationship) {
  def apply(m: Map[String, Any]): Any = relationship(m).asInstanceOf[Relationship].getType.name()

  override def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(RelationshipIdentifier(relationship.identifier.name))
  }
}

case class ArrayLengthValue(inner: Value) extends FunctionValue("LENGTH", inner) {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case path: Path => path.length()
    case x => throw new SyntaxException("Expected " + inner.identifier.name + " to be an iterable, but it is not.")
  }
}


case class IdValue(inner: Value) extends FunctionValue("ID", inner) {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
    case x => throw new SyntaxException("Expected " + inner.identifier.name + " to be a node or relationship.")
  }
}

case class PathNodesValue(path: EntityValue) extends FunctionValue("NODES", path) {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }
}

case class PathRelationshipsValue(path: EntityValue) extends FunctionValue("RELATIONSHIPS", path) {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }
}

case class EntityValue(entityName: String) extends Value {
  def apply(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException)

  def identifier: Identifier = Identifier(entityName)

  def checkAvailable(symbols: SymbolTable) {
    symbols.assertHas(Identifier(entityName))
  }
}

case class ParameterValue(parameterName: String) extends Value {
  def apply(m: Map[String, Any]): Any = m.getOrElse(parameterName, throw new ParameterNotFoundException("Expected a parameter named " + parameterName))

  def identifier: Identifier = Identifier(parameterName)

  def checkAvailable(symbols: SymbolTable) {
  }
}
