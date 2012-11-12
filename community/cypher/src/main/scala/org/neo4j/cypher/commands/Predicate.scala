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

import java.lang.String
import collection.Seq
import org.neo4j.cypher.symbols._
import scala.collection.JavaConverters._
import org.neo4j.graphdb.{DynamicRelationshipType, Node, Direction, PropertyContainer}
import org.neo4j.cypher.internal.pipes.Dependant
import java.util.regex.{Pattern=>RegexPattern}

abstract class Predicate extends Dependant {
  def ++(other: Predicate): Predicate = And(this, other)

  def isMatch(m: Map[String, Any]): Boolean

  // This is the un-dividable list of predicates. They can all be ANDed
  // together
  def atoms: Seq[Predicate]

  def containsIsNull: Boolean
}

case class And(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) && b.isMatch(m)

  def atoms: Seq[Predicate] = a.atoms ++ b.atoms

  def dependencies: Seq[Identifier] = a.dependencies ++ b.dependencies

  override def toString: String = "(" + a + " AND " + b + ")"

  def containsIsNull: Boolean = a.containsIsNull || b.containsIsNull
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) || b.isMatch(m)

  def atoms: Seq[Predicate] = Seq(this)

  def dependencies: Seq[Identifier] = a.dependencies ++ b.dependencies

  override def toString: String = "(" + a + " OR " + b + ")"

  def containsIsNull: Boolean = a.containsIsNull || b.containsIsNull
}

case class Not(a: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = !a.isMatch(m)

  def atoms: Seq[Predicate] = a.atoms.map(Not(_))

  def dependencies: Seq[Identifier] = a.dependencies

  override def toString: String = "NOT(" + a + ")"

  def containsIsNull: Boolean = a.containsIsNull
}

case class HasRelationshipTo(from: Expression, to: Expression, dir: Direction, relType: Option[String]) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = {
    val fromNode = from(m).asInstanceOf[Node]
    val toNode = to(m).asInstanceOf[Node]

    if ((fromNode == null) || (toNode == null)) {
      return false
    }

    relType match {
      case None => fromNode.getRelationships(dir).iterator().asScala.exists(rel => rel.getOtherNode(fromNode) == toNode)
      case Some(typ) => fromNode.getRelationships(dir, DynamicRelationshipType.withName(typ)).iterator().asScala.exists(rel => rel.getOtherNode(fromNode) == toNode)
    }
  }

  def atoms: Seq[Predicate] = Seq(this)

  def containsIsNull: Boolean = false

  def dependencies: Seq[Identifier] = from.dependencies(NodeType()) ++ to.dependencies(NodeType())
}

case class HasRelationship(from: Expression, dir: Direction, relType: Option[String]) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = {
    val fromNode = from(m).asInstanceOf[Node]

    if (fromNode == null) {
      return false
    }

    relType match {
      case None => fromNode.getRelationships(dir).iterator().hasNext
      case Some(typ) => fromNode.getRelationships(dir, DynamicRelationshipType.withName(typ)).iterator().hasNext
    }
  }


  def atoms: Seq[Predicate] = Seq(this)

  def containsIsNull: Boolean = false

  def dependencies: Seq[Identifier] = from.dependencies(NodeType())
}

case class IsNull(value: Expression) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = value(m) == null

  def dependencies: Seq[Identifier] = value.dependencies(AnyType())

  def atoms: Seq[Predicate] = Seq(this)

  override def toString: String = value + " IS NULL"

  def containsIsNull: Boolean = true
}

case class True() extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = true

  def dependencies: Seq[Identifier] = Seq()

  def atoms: Seq[Predicate] = Seq(this)

  override def toString: String = "true"

  def containsIsNull: Boolean = false
}

case class Has(property: Property) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = property match {
    case Property(identifier, propertyName) => {
      val propContainer = m(identifier).asInstanceOf[PropertyContainer]
      propContainer != null && propContainer.hasProperty(propertyName)
    }
  }

  def dependencies: Seq[Identifier] = property.dependencies(MapType())

  def atoms: Seq[Predicate] = Seq(this)

  override def toString: String = "hasProp(" + property + ")"

  def containsIsNull: Boolean = false
}

case class RegularExpression(a: Expression, regex: Expression) extends Predicate {

  val pat: (Map[String, Any]) => RegexPattern = getPattern

  private def getPattern: (Map[String, Any]) => RegexPattern = regex match {
    case Literal(x) => {
      val pattern = x.toString.r.pattern
      (x: Map[String, Any]) => pattern
    }
    case _ =>  (x: Map[String, Any]) => regex(x).toString.r.pattern
  }

  def isMatch(m: Map[String, Any]): Boolean = {
    val value = a.apply(m).asInstanceOf[String]
    getPattern(m).matcher(value).matches()
  }

  def dependencies: Seq[Identifier] = a.dependencies(StringType()) ++ regex.dependencies(StringType())

  def atoms: Seq[Predicate] = Seq(this)

  override def toString: String = a.toString() + " ~= /" + regex.toString() + "/"

  def containsIsNull: Boolean = false
}
