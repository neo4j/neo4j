/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.commands

import org.neo4j.cypher.internal.compiler.v2_2._
import commands.expressions.{Literal, Expression}
import commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{ReadsRelationshipProperty, ReadsLabel, ReadsNodeProperty, Effects}
import org.neo4j.cypher.internal.compiler.v2_2.helpers.{IsCollection, CollectionSupport, CastSupport}
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.compiler.v2_2.helpers._
import org.neo4j.graphdb._

abstract class Predicate extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState) = isMatch(ctx).getOrElse(null)
  def isTrue(m: ExecutionContext)(implicit state: QueryState): Boolean = isMatch(m).getOrElse(false)
  def ++(other: Predicate): Predicate = And.apply(this, other)
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean]

  // This is the un-dividable list of predicates. They can all be ANDed
  // together
  def atoms: Seq[Predicate] = Seq(this)
  def containsIsNull: Boolean
  protected def calculateType(symbols: SymbolTable) = CTBoolean

  def andWith(preds: Predicate*): Predicate = { preds match {
    case _ if preds.isEmpty => this
    case _                  => preds.fold(this)(_ ++ _)
  } }
}

object Predicate {
  def fromSeq(in: Seq[Predicate]) = in.reduceOption(_ ++ _).getOrElse(True())
}

object And {
  def apply(a: Predicate, b: Predicate) = (a, b) match {
    case (True(), other) => other
    case (other, True()) => other
    case (_, _)          => new And(a, b)
  }
}

case class Ands(predicates: List[Predicate]) extends Predicate {

  assert(predicates.nonEmpty, "Expected predicates to never be empty")

  def symbolTableDependencies: Set[String] = predicates.flatMap(_.symbolTableDependencies).toSet

  def containsIsNull: Boolean = predicates.exists(_.containsIsNull)

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    var result: Option[Option[Boolean]] = None
    val iter = predicates.iterator
    while (iter.nonEmpty) {
      val p = iter.next()
      val r = p.isMatch(m)

      if(r.nonEmpty && !r.get)
        return r

      if(result.isEmpty)
        result = Some(r)
      else {
        val stored = result.get
        if (stored.nonEmpty && stored.get && r.isEmpty)
          result = Some(None)
      }
    }

    result.get
  }

  def arguments: Seq[Expression] = predicates

  def rewrite(f: (Expression) => Expression): Expression = f(Ands(predicates.map(_.rewriteAsPredicate(f))))

  override def atoms: Seq[Predicate] = predicates
}


class And(val a: Predicate, val b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (a.isMatch(m), b.isMatch(m)) match {
    case (None, None)        => None
    case (Some(true), None)  => None
    case (Some(false), None) => Some(false)
    case (None, Some(true))  => None
    case (None, Some(false)) => Some(false)
    case (Some(l), Some(r))  => Some(l && r)
  }

  override def atoms: Seq[Predicate] = a.atoms ++ b.atoms
  override def toString: String = "(" + a + " AND " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(And(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  def arguments = Seq(a, b)

  override def hashCode() = a.hashCode + 37 * b.hashCode

  override def equals(p1: Any) = p1 match {
    case null       => false
    case other: And => a == other.a && b == other.b
    case _          => false
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Ors(predicates: List[Predicate]) extends Predicate {

  assert(predicates.nonEmpty, "Expected predicates to never be empty")

  def symbolTableDependencies: Set[String] = predicates.flatMap(_.symbolTableDependencies).toSet

  def containsIsNull: Boolean = predicates.exists(_.containsIsNull)

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {

    var result: Option[Option[Boolean]] = None

    val iter = predicates.iterator
    while (iter.nonEmpty) {
      val p = iter.next()
      val r = p.isMatch(m)

      if(r.nonEmpty && r.get)
        return r

      if(result.isEmpty)
        result = Some(r)
      else {
        val stored = result.get
        if (stored.nonEmpty && !stored.get && r.isEmpty)
          result = Some(None)
      }
    }

    result.get
  }

  def arguments: Seq[Expression] = predicates

  def rewrite(f: (Expression) => Expression): Expression = f(Ors(predicates.map(_.rewriteAsPredicate(f))))
}

case class Or(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (a.isMatch(m), b.isMatch(m)) match {
    case (None, None)        => None
    case (Some(true), None)  => Some(true)
    case (Some(false), None) => None
    case (None, Some(true))  => Some(true)
    case (None, Some(false)) => None
    case (Some(l), Some(r))  => Some(l || r)
  }

  override def toString: String = "(" + a + " OR " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Or(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Not(a: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = a.isMatch(m) match {
    case Some(x) => Some(!x)
    case None    => None
  }
  override def toString: String = "NOT(" + a + ")"
  def containsIsNull = a.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Not(a.rewriteAsPredicate(f)))
  def arguments = Seq(a)
  def symbolTableDependencies = a.symbolTableDependencies
}

case class Xor(a: Predicate, b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (a.isMatch(m), b.isMatch(m)) match {
    case (None, _)          => None
    case (_, None)          => None
    case (Some(l), Some(r)) => Some(l ^ r)
  }

  override def toString: String = "(" + a + " XOR " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = f(Xor(a.rewriteAsPredicate(f), b.rewriteAsPredicate(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class IsNull(expression: Expression) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = expression(m) match {
    case null => Some(true)
    case _    => Some(false)
  }

  override def toString: String = expression + " IS NULL"
  def containsIsNull = true
  def rewrite(f: (Expression) => Expression) = f(IsNull(expression.rewrite(f)))
  def arguments = Seq(expression)
  def symbolTableDependencies = expression.symbolTableDependencies
}

case class True() extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = Some(true)
  override def toString: String = "true"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = f(this)
  def arguments = Nil
  def symbolTableDependencies = Set()
}

case class PropertyExists(identifier: Expression, propertyKey: KeyToken) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = identifier(m) match {
    case pc: Node         => Some(propertyKey.getOptId(state.query).exists(state.query.nodeOps.hasProperty(pc.getId, _)))
    case pc: Relationship => Some(propertyKey.getOptId(state.query).exists(state.query.relationshipOps.hasProperty(pc.getId, _)))
    case null             => None
    case _                => throw new CypherTypeException("Expected " + identifier + " to be a property container.")
  }

  override def toString: String = s"hasProp($identifier.${propertyKey.name})"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = f(PropertyExists(identifier.rewrite(f), propertyKey.rewrite(f)))

  def arguments = Seq(identifier)

  def symbolTableDependencies = identifier.symbolTableDependencies

  override def localEffects(symbols: SymbolTable) = Effects.propertyRead(identifier, symbols)(propertyKey.name)
}

case class LiteralRegularExpression(a: Expression, regex: Literal) extends Predicate {
  lazy val pattern = regex.v.asInstanceOf[String].r.pattern

  def isMatch(m: ExecutionContext)(implicit state: QueryState) = Option(a(m)).map {
    x =>
      val v = CastSupport.castOrFail[String](x)
      pattern.matcher(v).matches()
  }

  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = f(regex.rewrite(f) match {
    case lit: Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other        => RegularExpression(a.rewrite(f), other)
  })

  def arguments = Seq(a, regex)

  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class RegularExpression(a: Expression, regex: Expression) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = (a(m), regex(m)) match {
    case (null, _) => None
    case (_, null) => None
    case (a1, r1)  =>
      val a2 = CastSupport.castOrFail[String](a1)
      val r2 = CastSupport.castOrFail[String](r1)
      Some(r2.r.pattern.matcher(a2).matches())
  }

  override def toString: String = a.toString() + " ~= /" + regex.toString() + "/"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = f(regex.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other => RegularExpression(a.rewrite(f), other)
  })

  def arguments = Seq(a, regex)

  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class NonEmpty(collection: Expression) extends Predicate with CollectionSupport {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    collection(m) match {
      case IsCollection(x) => Some(x.nonEmpty)
      case null            => None
      case x               => throw new CypherTypeException("Expected a collection, got `%s`".format(x))
    }
  }

  override def toString: String = "nonEmpty(" + collection.toString() + ")"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = f(NonEmpty(collection.rewrite(f)))
  def arguments = Seq(collection)
  def symbolTableDependencies = collection.symbolTableDependencies
}

case class HasLabel(entity: Expression, label: KeyToken) extends Predicate {

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = entity(m) match {

    case null =>
      None

    case value =>
      val node           = CastSupport.castOrFail[Node](value)
      val nodeId         = node.getId
      val queryCtx       = state.query

      label.getOptId(state.query) match {
        case None =>
          Some(false)
        case Some(labelId) =>
          Some(queryCtx.isLabelSetOnNode(labelId, nodeId))
      }
  }

  override def toString = s"$entity:${label.name}"

  def rewrite(f: (Expression) => Expression) = f(HasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f)))

  override def children = Seq(label, entity)

  def arguments: Seq[Expression] = Seq(entity)

  def symbolTableDependencies = entity.symbolTableDependencies ++ label.symbolTableDependencies

  def containsIsNull = false

  override def localEffects(symbols: SymbolTable) = Effects(ReadsLabel(label.name))
}

case class CoercedPredicate(inner:Expression) extends Predicate with CollectionSupport {
  def arguments = Seq(inner)

  def isMatch(m: ExecutionContext)(implicit state: QueryState) = inner(m) match {
    case x: Boolean         => Some(x)
    case null               => None
    case IsCollection(coll) => Some(coll.nonEmpty)
    case x                  => throw new CypherTypeException(s"Don't know how to treat that as a predicate: $x")
  }

  def rewrite(f: (Expression) => Expression) = f(CoercedPredicate(inner.rewrite(f)))

  def containsIsNull = false

  def symbolTableDependencies = inner.symbolTableDependencies

  override def toString = inner.toString
}
