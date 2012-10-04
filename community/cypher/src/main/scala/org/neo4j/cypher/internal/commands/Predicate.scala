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

import expressions.{Literal, Property, Expression}
import java.lang.String
import collection.Seq
import scala.collection.JavaConverters._
import org.neo4j.graphdb.{DynamicRelationshipType, Node, Direction, PropertyContainer}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.helpers.ThisShouldNotHappenError
import collection.Map
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.helpers.{IsCollection, CollectionSupport}

abstract class Predicate extends Expression {
  def apply(m: Map[String, Any]) = isMatch(m)
  def ++(other: Predicate): Predicate = And(this, other)
  def isMatch(m: Map[String, Any]): Boolean

  // This is the un-dividable list of predicates. They can all be ANDed
  // together
  def atoms: Seq[Predicate]
  def rewrite(f: Expression => Expression): Predicate
  def containsIsNull: Boolean
  def filter(f: Expression => Boolean): Seq[Expression]
  def assertInnerTypes(symbols: SymbolTable)
  protected def calculateType(symbols: SymbolTable) = {
    assertInnerTypes(symbols)
    BooleanType()
  }
}

case class NullablePredicate(inner: Predicate, exp: Seq[(Expression, Boolean)]) extends Predicate {
  def isMatch(m: Map[String, Any]) = {
    val nullValue = exp.find {
      case (e, res) => e(m) == null
    }

    nullValue match {
      case Some((_, res)) => res
      case _ => inner.isMatch(m)
    }
  }

  def atoms = Seq(this)
  override def toString() = "nullable([" + exp.mkString(",") +"],["  +inner.toString+"])"
  def containsIsNull = inner.containsIsNull

  def rewrite(f: (Expression) => Expression) = NullablePredicate(inner.rewrite(f), exp.map {
    case (e, ascDesc) => (e.rewrite(f), ascDesc)
  })

  def filter(f: (Expression) => Boolean) = exp.flatMap { case (e,_) => e.filter(f)  }

  def assertInnerTypes(symbols: SymbolTable) {
    inner.assertTypes(symbols)
  }

  def symbolTableDependencies = inner.symbolTableDependencies ++ exp.flatMap(_._1.symbolTableDependencies).toSet
}


case class And(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) && b.isMatch(m)
  def atoms: Seq[Predicate] = a.atoms ++ b.atoms
  override def toString(): String = "(" + a + " AND " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = And(a.rewrite(f), b.rewrite(f))
  def filter(f: (Expression) => Boolean) = a.filter(f) ++ b.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    a.assertTypes(symbols)
    b.assertTypes(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) || b.isMatch(m)
  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = "(" + a + " OR " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = Or(a.rewrite(f), b.rewrite(f))
  def filter(f: (Expression) => Boolean) = a.filter(f) ++ b.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    a.assertTypes(symbols)
    b.assertTypes(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Not(a: Predicate) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = !a.isMatch(m)
  def atoms: Seq[Predicate] = a.atoms.map(Not(_))
  override def toString(): String = "NOT(" + a + ")"
  def containsIsNull = a.containsIsNull
  def rewrite(f: (Expression) => Expression) = Not(a.rewrite(f))
  def filter(f: (Expression) => Boolean) = a.filter(f)
  def assertInnerTypes(symbols: SymbolTable) {
    a.assertTypes(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies
}

case class HasRelationshipTo(from: Expression, to: Expression, dir: Direction, relType: Seq[String]) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = {
    val fromNode = from(m).asInstanceOf[Node]
    val toNode = to(m).asInstanceOf[Node]

    if ((fromNode == null) || (toNode == null)) {
      return false
    }

    if(relType.isEmpty) {
      fromNode.getRelationships(dir).iterator().asScala.exists(rel => rel.getOtherNode(fromNode) == toNode)
    } else {
      val types = relType.map(t=>  DynamicRelationshipType.withName(t))
      val rels = fromNode.getRelationships(dir, types: _*).iterator().asScala
      rels.exists(rel => rel.getOtherNode(fromNode) == toNode)
    }
  }

  def atoms: Seq[Predicate] = Seq(this)
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = HasRelationshipTo(from.rewrite(f), to.rewrite(f), dir, relType)
  def filter(f: (Expression) => Boolean) = from.filter(f) ++ to.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    from.assertTypes(symbols)
    to.assertTypes(symbols)
  }
  def symbolTableDependencies = from.symbolTableDependencies ++ to.symbolTableDependencies
}

case class HasRelationship(from: Expression, dir: Direction, relType: Seq[String]) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = {
    val fromNode = from(m).asInstanceOf[Node]

    if (fromNode == null) {
      return false
    }


    if (relType.isEmpty)
      fromNode.getRelationships(dir).iterator().hasNext
    else
      fromNode.getRelationships(dir, relType.map(t => DynamicRelationshipType.withName(t)): _*).iterator().hasNext
  }

  def atoms: Seq[Predicate] = Seq(this)
  def containsIsNull = false
  def filter(f: (Expression) => Boolean) = from.filter(f)
  def rewrite(f: (Expression) => Expression) = HasRelationship(from.rewrite(f), dir, relType)
  def assertInnerTypes(symbols: SymbolTable) {
    from.assertTypes(symbols)
  }

  def symbolTableDependencies = from.symbolTableDependencies
}

case class IsNull(expression: Expression) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = expression(m) == null
  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = expression + " IS NULL"
  def containsIsNull = true
  def rewrite(f: (Expression) => Expression) = IsNull(expression.rewrite(f))
  def filter(f: (Expression) => Boolean) = expression.filter(f)
  def assertInnerTypes(symbols: SymbolTable) {
    expression.assertTypes(symbols)
  }
  def symbolTableDependencies = expression.symbolTableDependencies
}

case class True() extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = true
  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = "true"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = True()
  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this)
  else
    Seq()
  def assertInnerTypes(symbols: SymbolTable) {}
  def symbolTableDependencies = Set()
}

case class Has(property: Property) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = property match {
    case Property(identifier, propertyName) => {
      val propContainer = m(identifier).asInstanceOf[PropertyContainer]
      propContainer != null && propContainer.hasProperty(propertyName)
    }
  }

  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = "hasProp(" + property + ")"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = property.rewrite(f) match {
    case prop:Property => Has(prop)
    case _ => throw new ThisShouldNotHappenError("Andres", "Something went wrong rewriting a Has(Property)")
  }
  def filter(f: (Expression) => Boolean) = property.filter(f)
  def assertInnerTypes(symbols: SymbolTable) {
    property.assertTypes(symbols)
  }

  def symbolTableDependencies = property.symbolTableDependencies
}

case class LiteralRegularExpression(a: Expression, regex: Literal) extends Predicate {
  lazy val pattern = regex(Map()).asInstanceOf[String].r.pattern
  
  def isMatch(m: Map[String, Any]) = pattern.matcher(a(m).asInstanceOf[String]).matches()
  def atoms = Seq(this)
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = regex.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other => RegularExpression(a.rewrite(f), other)
  }
  def filter(f: (Expression) => Boolean) = a.filter(f) ++ regex.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    a.evaluateType(StringType(), symbols)
    regex.evaluateType(StringType(), symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class RegularExpression(a: Expression, regex: Expression) extends Predicate {
  def isMatch(m: Map[String, Any]): Boolean = {
    val value = a(m).asInstanceOf[String]
    val regularExp = regex(m).asInstanceOf[String]

    regularExp.r.pattern.matcher(value).matches()
  }

  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = a.toString() + " ~= /" + regex.toString() + "/"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = regex.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other => RegularExpression(a.rewrite(f), other)
  }
  def filter(f: (Expression) => Boolean) = a.filter(f) ++ regex.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    a.evaluateType(StringType(), symbols)
    regex.evaluateType(StringType(), symbols)
  }
  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class NonEmpty(collection:Expression) extends Predicate with CollectionSupport {
  def isMatch(m: Map[String, Any]): Boolean = {
    collection(m) match {
      case IsCollection(x) => this.makeTraversable(collection(m)).nonEmpty
      case null          => false
      case x             => throw new CypherTypeException("Expected a collection, got `%s`".format(x))
    }
  }

  def atoms: Seq[Predicate] = Seq(this)
  override def toString(): String = "nonEmpty(" + collection + ")"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = NonEmpty(collection.rewrite(f))
  def filter(f: (Expression) => Boolean) = collection.filter(f)

  def assertInnerTypes(symbols: SymbolTable) {
    collection.evaluateType(AnyCollectionType(), symbols)
  }

  def symbolTableDependencies = collection.symbolTableDependencies
}
