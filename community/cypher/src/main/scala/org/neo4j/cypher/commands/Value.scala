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

import scala.collection.JavaConverters._
import java.lang.String
import org.neo4j.cypher._
import symbols._
import org.neo4j.graphdb.{Path, Relationship, NotFoundException, PropertyContainer, Node}
import collection.Seq

abstract class Value extends (Map[String, Any] => Any) {
  def identifier: Identifier

  def declareDependencies(extectedType: AnyType): Seq[Identifier]

  def dependencies(extectedType: AnyType): Seq[Identifier] = {
    if (!extectedType.isAssignableFrom(identifier.typ))
      throw new SyntaxException(identifier.name + " expected to be of type " + extectedType + " but it is of type " + identifier.typ)
    declareDependencies(extectedType)
  }
}

//TODO: This should not be a castable value
case class Literal(v: Any) extends CastableValue {
  def apply(m: Map[String, Any]) = v

  def identifier = Identifier(v.toString, ScalarType())

  override def toString() = if (v.isInstanceOf[String]) "\"" + v + "\"" else v.toString

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()
}

case class NullablePropertyValue(subEntity: String, subProperty: String) extends PropertyValue(subEntity, subProperty) {
  protected override def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = null
}

abstract class CastableValue extends Value {
  override def dependencies(extectedType: AnyType): Seq[Identifier] = declareDependencies(extectedType)
}

case class PropertyValue(entity: String, property: String) extends CastableValue {
  protected def handleNotFound(propertyContainer: PropertyContainer, x: NotFoundException): Any = throw new SyntaxException("%s.%s does not exist on %s".format(entity, property, propertyContainer), x)

  def apply(m: Map[String, Any]): Any = {
    m(entity).asInstanceOf[PropertyContainer] match {
      case null => null
      case propertyContainer => try {
        propertyContainer.getProperty(property)
      } catch {
        case x: NotFoundException => handleNotFound(propertyContainer, x)
      }
    }
  }

  def identifier: Identifier = Identifier(entity + "." + property, ScalarType())

  override def toString(): String = entity + "." + property

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entity, MapType()))
}

case class RelationshipTypeValue(relationship: Value) extends Value {
  def apply(m: Map[String, Any]): Any = relationship(m).asInstanceOf[Relationship].getType.name()

  def identifier = Identifier("TYPE(" + relationship.identifier.name + ")", StringType())

  override def toString() = "type(" + relationship + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = relationship.dependencies(RelationshipType())
}

case class CoalesceValue(values: Value*) extends Value {
  def apply(m: Map[String, Any]): Any = values.map(valueObject => valueObject(m)).find(value => value != null) match {
    case None => null
    case Some(x) => x
  }

  def innerExpectedType: Option[AnyType] = null

  def argumentsString: String = values.map(_.identifier.name).mkString(",")

  //TODO: Find out the closest matching return type
  def identifier = Identifier("COALESCE(" + argumentsString + ")", AnyType())

  override def toString() = "coalesce(" + argumentsString + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = values.flatMap(_.dependencies(AnyType()))
}

case class ArrayLengthValue(inner: Value) extends Value {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case path: Path => path.length()
    case iter: Traversable[_] => iter.toList.length
    case s: String => s.length()
    case x => throw new IterableRequiredException(inner)
  }

  def identifier = Identifier("LENGTH(" + inner.identifier.name + ")", IntegerType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(AnyIterableType())
}

case class IdValue(inner: Value) extends Value {
  def apply(m: Map[String, Any]): Any = inner(m) match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
  }

  def identifier = Identifier("ID(" + inner.identifier.name + ")", LongType())


  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(MapType())
}

case class PathNodesValue(path: Value) extends Value {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("NODES(" + path.identifier.name + ")", new IterableType(NodeType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())
}

case class Extract(iterable: Value, id: String, expression: Value) extends Value {
  def apply(m: Map[String, Any]): Any = iterable(m) match {
    case x: Iterable[Any] => x.map(iterValue => {
      val innerMap = m + (id -> iterValue)
      expression(innerMap)
    })
    case _ => throw new IterableRequiredException(iterable)
  }

  def identifier = Identifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")", new IterableType(expression.identifier.typ))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] =
  // Extract depends on everything that the iterable and the expression depends on, except
  // the new identifier inserted into the expression context, named with id
    iterable.dependencies(AnyIterableType()) ++ expression.dependencies(AnyType()).filterNot(_.name == id)
}

case class PathRelationshipsValue(path: Value) extends Value {
  def apply(m: Map[String, Any]): Any = path(m) match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("RELATIONSHIPS(" + path.identifier.name + ")", new IterableType(RelationshipType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

}

case class EntityValue(entityName: String) extends CastableValue {
  def apply(m: Map[String, Any]): Any = m.getOrElse(entityName, throw new NotFoundException)

  def identifier: Identifier = Identifier(entityName, AnyType())

  override def toString(): String = entityName

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq(Identifier(entityName, extectedType))
}

case class ParameterValue(parameterName: String) extends CastableValue {
  def apply(m: Map[String, Any]): Any = m.getOrElse(parameterName, throw new ParameterNotFoundException("Expected a parameter named " + parameterName))

  def identifier: Identifier = Identifier(parameterName, AnyType())

  override def toString(): String = "{" + parameterName + "}"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = Seq()
}