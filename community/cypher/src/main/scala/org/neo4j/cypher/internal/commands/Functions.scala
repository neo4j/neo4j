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
import collection.Map

trait Functions

abstract class NullInNullOutExpression(argument: Expression) extends Expression {
  def compute(value: Any, m: Map[String, Any]): Any

  def compute(m: Map[String, Any]): Any = argument(m) match {
    case null => null
    case x => compute(x, m)
  }
}

case class ExtractFunction(iterable: Expression, id: String, expression: Expression) extends NullInNullOutExpression(iterable) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case x: Iterable[Any] => x.map(iterValue => {
      val innerMap = m + (id -> iterValue)
      expression(innerMap)
    }).toList
    case _ => throw new IterableRequiredException(iterable)
  }

  val identifier = Identifier("extract(" + id + " in " + iterable.identifier.name + " : " + expression.identifier.name + ")", new IterableType(expression.identifier.typ))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] =
  // Extract depends on everything that the iterable and the expression depends on, except
  // the new identifier inserted into the expression context, named with id
    iterable.dependencies(AnyIterableType()) ++ expression.dependencies(AnyType()).filterNot(_.name == id)

  def rewrite(f: (Expression) => Expression) = f(ExtractFunction(iterable.rewrite(f), id, expression.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ iterable.filter(f) ++ expression.filter(f)
  else
    iterable.filter(f) ++ expression.filter(f)
}

case class RelationshipFunction(path: Expression) extends NullInNullOutExpression(path) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case p: Path => p.relationships().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  val identifier = Identifier("RELATIONSHIPS(" + path.identifier.name + ")", new IterableType(RelationshipType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipFunction(path.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ path.filter(f)
  else
    path.filter(f)
}

case class CoalesceFunction(expressions: Expression*) extends Expression {
  def compute(m: Map[String, Any]): Any = expressions.toStream.map(expression => expression(m)).find(value => value != null) match {
    case None => null
    case Some(x) => x
  }

  def innerExpectedType: Option[AnyType] = None

  val argumentsString: String = expressions.map(_.identifier.name).mkString(",")

  //TODO: Find out the closest matching return type
  val identifier = Identifier("COALESCE(" + argumentsString + ")", AnyType())

  override def toString() = "coalesce(" + argumentsString + ")"

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = expressions.flatMap(_.dependencies(AnyType()))

  def rewrite(f: (Expression) => Expression) = f(CoalesceFunction(expressions.map(e => e.rewrite(f)): _*))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ expressions.flatMap(_.filter(f))
  else
    expressions.flatMap(_.filter(f))
}

case class RelationshipTypeFunction(relationship: Expression) extends NullInNullOutExpression(relationship) {
  def compute(value: Any, m: Map[String, Any]) = value.asInstanceOf[Relationship].getType.name()

  lazy val identifier = Identifier("TYPE(" + relationship.identifier.name + ")", StringType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = relationship.dependencies(RelationshipType())

  def rewrite(f: (Expression) => Expression) = f(RelationshipTypeFunction(relationship.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ relationship.filter(f)
  else
    relationship.filter(f)
}

case class LengthFunction(inner: Expression) extends NullInNullOutExpression(inner) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case path: Path => path.length()
    case iter: Traversable[_] => iter.toList.length
    case s: String => s.length()
    case x => throw new IterableRequiredException(inner)
  }

  val identifier = Identifier("LENGTH(" + inner.identifier.name + ")", IntegerType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = {
    val seq = inner.dependencies(AnyIterableType()).toList
    seq
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ inner.filter(f)
  else
    inner.filter(f)
}

case class IdFunction(inner: Expression) extends NullInNullOutExpression(inner) {

  def compute(value: Any, m: Map[String, Any]) = value match {
    case node: Node => node.getId
    case rel: Relationship => rel.getId
  }

  val identifier = Identifier("ID(" + inner.identifier.name + ")", LongType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = inner.dependencies(MapType())

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ inner.filter(f)
  else
    inner.filter(f)
}

case class HeadFunction(collection: Expression) extends NullInNullOutExpression(collection) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case path: Path => path.startNode()
    case iter: Traversable[_] => iter.head
    case array: Array[_] => array.head
    case x => throw new IterableRequiredException(collection)
  }

  private def myType = collection.identifier.typ match {
    case x: IterableType => x.iteratedType
    case _ => ScalarType()
  }

  val identifier = Identifier("head(" + collection.identifier.name + ")", myType)

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = collection.dependencies(AnyIterableType())

  def rewrite(f: (Expression) => Expression) = f(HeadFunction(collection.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ collection.filter(f)
  else
    collection.filter(f)
}

case class LastFunction(collection: Expression) extends NullInNullOutExpression(collection) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case path: Path => path.endNode()
    case iter: Traversable[_] => iter.last
    case array: Array[_] => array.last
    case x => throw new IterableRequiredException(collection)
  }

  val identifier = Identifier("last(" + collection.identifier.name + ")", ScalarType())

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = collection.dependencies(AnyIterableType())

  def rewrite(f: (Expression) => Expression) = f(LastFunction(collection.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ collection.filter(f)
  else
    collection.filter(f)
}

case class TailFunction(collection: Expression) extends NullInNullOutExpression(collection) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case path: Path => path.iterator().asScala.toSeq.tail
    case iter: Traversable[_] => iter.tail
    case array: Array[_] => array.tail
    case x => throw new IterableRequiredException(collection)
  }

  val identifier = Identifier("tail(" + collection.identifier.name + ")", collection.identifier.typ)

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = collection.dependencies(AnyIterableType())

  def rewrite(f: (Expression) => Expression) = f(TailFunction(collection.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ collection.filter(f)
  else
    collection.filter(f)
}

case class NodesFunction(path: Expression) extends NullInNullOutExpression(path) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case p: Path => p.nodes().asScala.toSeq
    case x => throw new SyntaxException("Expected " + path.identifier.name + " to be a path.")
  }

  val identifier = Identifier("NODES(" + path.identifier.name + ")", new IterableType(NodeType()))

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = path.dependencies(PathType())

  def rewrite(f: (Expression) => Expression) = f(NodesFunction(path.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ path.filter(f)
  else
    path.filter(f)
}

case class FilterFunction(collection: Expression, symbol: String, predicate: Predicate) extends NullInNullOutExpression(collection) {
  def compute(value: Any, m: Map[String, Any]) = {
    val seq = value match {
      case path: Path => path.iterator().asScala.toSeq
      case iter: Traversable[_] => iter.toSeq
      case array: Array[_] => array.toSeq
      case x => throw new IterableRequiredException(collection)
    }

    seq.filter(element => predicate.isMatch(m + (symbol -> element)))
  }

  val identifier = Identifier("filter(%s in %s : %s)".format(symbol, collection.identifier.name, predicate), collection.identifier.typ)

  def declareDependencies(extectedType: AnyType): Seq[Identifier] = (collection.dependencies(PathType()) ++ predicate.dependencies).filterNot(_.name == symbol)

  def rewrite(f: (Expression) => Expression) = f(FilterFunction(collection.rewrite(f), symbol, predicate.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ collection.filter(f)
  else
    collection.filter(f)
}
