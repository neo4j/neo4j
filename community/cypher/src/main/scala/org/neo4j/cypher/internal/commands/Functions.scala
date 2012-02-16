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

import org.neo4j.cypher.{SyntaxException, IterableRequiredException}
import scala.collection.JavaConverters._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.graphdb.{Node, Relationship, Path}

trait Functions

abstract class NullInNullOutExpression(argument: Expression) extends Expression {
  def inner_apply(value: Any, m: Map[String, Any]): Any

  def apply(m: Map[String, Any]): Any = argument(m) match {
    case null => null
    case x => inner_apply(x, m)
  }
}

case class ExtractFunction(iterable: Expression, id: String, expression: Expression) extends NullInNullOutExpression(iterable) {
  def inner_apply(value: Any, m: Map[String, Any]) = value match {
    case x: Iterable[Any] => x.map(iterValue => {
      val innerMap = m + (id -> iterValue)
      expression(innerMap)
    }).toList
    case _ => throw new IterableRequiredException(iterable)
  }

  def identifier = Identifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")", new IterableType(expression.identifier.typ))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] =
  // Extract depends on everything that the iterable and the expression depends on, except
  // the new identifier inserted into the expression context, named with id
    iterable.dependencies(AnyIterableType()) ++ expression.dependencies(AnyType()).filterNot(_.name == id)

  def rewrite(f: (Expression) => Expression) = f(ExtractFunction(iterable.rewrite(f), id, expression.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || iterable.exists(f) || expression.exists(f)
}

case class RelationshipFunction(path: Expression) extends NullInNullOutExpression(path) {
  def inner_apply(value: Any, m: Map[String, Any]) = value match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("RELATIONSHIPS(" + path.identifier.name + ")", new IterableType(RelationshipType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipFunction(path.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || path.exists(f)
}

case class CoalesceFunction(expressions: Expression*) extends Expression {
  def apply(m: Map[String, Any]): Any = expressions.map(expression => expression(m)).find(value => value != null) match {
    case None => null
    case Some(x) => x
  }

  def innerExpectedType: Option[AnyType] = null

  def argumentsString: String = expressions.map(_.identifier.name).mkString(",")

  //TODO: Find out the closest matching return type
  def identifier = Identifier("COALESCE(" + argumentsString + ")", AnyType())

  override def toString() = "coalesce(" + argumentsString + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = expressions.flatMap(_.dependencies(AnyType()))

  def rewrite(f: (Expression) => Expression) = f(CoalesceFunction(expressions.map(e => e.rewrite(f)): _*))

  def exists(f: (Expression) => Boolean) = f(this) || expressions.exists(_.exists(f))
}

case class RelationshipTypeFunction(relationship: Expression) extends NullInNullOutExpression(relationship) {
  def inner_apply(value: Any, m: Map[String, Any]) = value.asInstanceOf[Relationship].getType.name()

  def identifier = Identifier("TYPE(" + relationship.identifier.name + ")", StringType())

  override def toString() = "type(" + relationship + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = relationship.dependencies(RelationshipType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipTypeFunction(relationship.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || relationship.exists(f)
}

case class LengthFunction(inner: Expression) extends NullInNullOutExpression(inner) {
  def inner_apply(value: Any, m: Map[String, Any]) = value match {
    case path: Path => path.length()
    case iter: Traversable[_] => iter.toList.length
    case s: String => s.length()
    case x => throw new IterableRequiredException(inner)
  }

  def identifier = Identifier("LENGTH(" + inner.identifier.name + ")", IntegerType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = {
    val seq = inner.dependencies(AnyIterableType()).toList
    seq
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || inner.exists(f)
}

case class IdFunction(inner: Expression) extends NullInNullOutExpression(inner) {

  def inner_apply(value: Any, m: Map[String, Any]) = value match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
  }

  def identifier = Identifier("ID(" + inner.identifier.name + ")", LongType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(MapType())

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(this) || inner.exists(f)
}

case class NodesFunction(path: Expression) extends NullInNullOutExpression(path) {
  def inner_apply(value: Any, m: Map[String, Any]) = value match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  def identifier = Identifier("NODES(" + path.identifier.name + ")", new IterableType(NodeType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(NodesFunction(path.rewrite(f)))

  def exists(f: (Expression) => Boolean) = f(path) || path.exists(f)
}
