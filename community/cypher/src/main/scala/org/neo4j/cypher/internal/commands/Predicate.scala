/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.graphdb._
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.CypherTypeException
import org.neo4j.cypher.internal.helpers.{CastSupport, IsCollection, CollectionSupport}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.commands.values._
import org.neo4j.cypher.internal.commands.expressions.Literal
import scala.Some
import org.neo4j.cypher.internal.symbols.AnyType
import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.cypher.internal.pipes.QueryState

abstract class Predicate extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState): Boolean = isMatch(ctx)
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean

  def ternaryApply(ctx: ExecutionContext)(implicit state: QueryState): Ternary = Ternary(isMatch(ctx))
  def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = Ternary(apply(m))

  def ++(other: Predicate): Predicate = And.apply(this, other)

  // This is the un-dividable list of predicates. They can all be ANDed
  // together
  // implement ternary logic in terms of binary logic
  def atoms: Seq[Predicate] = Seq(this)
  def rewrite(f: Expression => Expression): Predicate
  def containsIsNull: Boolean
  def assertInnerTypes(symbols: SymbolTable)
  protected def calculateType(symbols: SymbolTable) = {
    assertInnerTypes(symbols)
    BooleanType()
  }

  def andWith(preds: Predicate*): Predicate = { preds match {
    case _ if preds.isEmpty => this
    case _ => preds.fold(this)(_ ++ _)
  } }

}

abstract class TernaryPredicate extends Predicate {
  // implement binary logic in terms of ternary logic

  override final def apply(ctx: ExecutionContext)(implicit state: QueryState): Boolean =
    ternaryApply(ctx).isTrueOrNotApplicable

  override final def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean =
    ternaryIsMatch(m).isTrueOrNotApplicable

  override def ternaryApply(ctx: ExecutionContext)(implicit state: QueryState): Ternary = ternaryIsMatch(ctx)
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary
}

object Predicate {
  def fromSeq(in: Seq[Predicate]) = in.reduceOption(_ ++ _).getOrElse(True())
}

case class NullablePredicate(inner: Predicate, exp: Seq[(Expression, Boolean)]) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary =  {
    val nullable = exp.find { case (e, res) => NotBound.orNull(e(m)) }
    nullable match {
      case Some((_, res)) => Ternary(res)
      case _              => inner.ternaryIsMatch(m)
    }
  }

  override def toString() = "nullable([" + exp.mkString(",") +"],["  +inner.toString+"])"
  def containsIsNull = inner.containsIsNull

  def rewrite(f: (Expression) => Expression) = NullablePredicate(inner.rewrite(f), exp.map {
    case (e, ascDesc) => (e.rewrite(f), ascDesc)
  })

  def children = Seq(inner) ++ exp.map(_._1)

  def assertInnerTypes(symbols: SymbolTable) {
    inner.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = inner.symbolTableDependencies ++ exp.flatMap(_._1.symbolTableDependencies).toSet
}

object And {
  def apply(a: Predicate, b: Predicate) = (a, b) match {
    case (True(), other) => other
    case (other, True()) => other
    case (_, _)          => new And(a, b)
  }
}

class And(val a: Predicate, val b: Predicate) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary =
    a.ternaryIsMatch(m) `and` b.ternaryIsMatch(m)

  override def atoms: Seq[Predicate] = a.atoms ++ b.atoms
  override def toString(): String = "(" + a + " AND " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = And(a.rewrite(f), b.rewrite(f))

  def children = Seq(a, b)

  def assertInnerTypes(symbols: SymbolTable) {
    a.throwIfSymbolsMissing(symbols)
    b.throwIfSymbolsMissing(symbols)
  }

  override def equals(p1: Any) = p1 match {
    case null       => false
    case other: And => a == other.a && b == other.b
    case _          => false
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Or(a: Predicate, b: Predicate) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary =
    a.ternaryIsMatch(m) `or` b.ternaryIsMatch(m)

  override def toString(): String = "(" + a + " OR " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = Or(a.rewrite(f), b.rewrite(f))

  def children = Seq(a, b)

  def assertInnerTypes(symbols: SymbolTable) {
    a.throwIfSymbolsMissing(symbols)
    b.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class Not(a: Predicate) extends TernaryPredicate {
  override def atoms: Seq[Predicate] = a.atoms.map(Not(_))
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary =
    a.ternaryIsMatch(m).negated

  override def toString(): String = "NOT(" + a + ")"
  def containsIsNull = a.containsIsNull
  def rewrite(f: (Expression) => Expression) = Not(a.rewrite(f))
  def children = Seq(a)
  def assertInnerTypes(symbols: SymbolTable) {
    a.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies
}

case class Xor(a: Predicate, b: Predicate) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary =
    a.ternaryIsMatch(m) `xor` b.ternaryIsMatch(m)

  override def toString(): String = "(" + a + " XOR " + b + ")"
  def containsIsNull = a.containsIsNull||b.containsIsNull
  def rewrite(f: (Expression) => Expression) = Xor(a.rewrite(f), b.rewrite(f))

  def children = Seq(a, b)

  def assertInnerTypes(symbols: SymbolTable) {
    a.throwIfSymbolsMissing(symbols)
    b.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class HasRelationshipTo(from: Expression, to: Expression, dir: Direction, relType: Seq[String])
  extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = {
    val fromVal = from(m)
    val toVal = to(m)

    if ((fromVal == null) || (toVal == null)) {
      return IsFalse
    }

    if (NotBound(fromVal) || NotBound(toVal)) {
      return NotApplicable
    }

    val fromNode = fromVal.asInstanceOf[Node]
    val toNode = toVal.asInstanceOf[Node]

    val result =
      state.query.getRelationshipsFor(fromNode, dir, relType).exists(rel => rel.getOtherNode(fromNode) == toNode)

    Ternary(result)
  }

  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = HasRelationshipTo(from.rewrite(f), to.rewrite(f), dir, relType)
  def children = Seq(from, to)
  def assertInnerTypes(symbols: SymbolTable) {
    from.throwIfSymbolsMissing(symbols)
    to.throwIfSymbolsMissing(symbols)
  }
  def symbolTableDependencies = from.symbolTableDependencies ++ to.symbolTableDependencies
}

case class HasRelationship(from: Expression, dir: Direction, relType: Seq[String]) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = from(m) match {
    case null =>
      IsFalse

    case NotBound =>
      NotApplicable

    case fromNode: Node =>
      val matchingRelationships = state.query.getRelationshipsFor(fromNode, dir, relType)
      Ternary(matchingRelationships.hasNext)
  }

  def containsIsNull = false
  def children = Seq(from)
  def rewrite(f: (Expression) => Expression) = HasRelationship(from.rewrite(f), dir, relType)
  def assertInnerTypes(symbols: SymbolTable) {
    from.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = from.symbolTableDependencies
}

case class IsNull(expression: Expression) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = NotBound.orNull(expression(m))

  override def toString(): String = expression + " IS NULL"
  def containsIsNull = true
  def rewrite(f: (Expression) => Expression) = IsNull(expression.rewrite(f))
  def children = Seq(expression)
  def assertInnerTypes(symbols: SymbolTable) {
    expression.throwIfSymbolsMissing(symbols)
  }
  def symbolTableDependencies = expression.symbolTableDependencies
}

case class True() extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = true
  override def toString(): String = "true"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = True()
  def children = Nil
  def assertInnerTypes(symbols: SymbolTable) {}
  def symbolTableDependencies = Set()
}

case class Has(identifier: Expression, propertyKey: KeyToken) extends TernaryPredicate {
  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = identifier(m) match {
    case pc: Node         =>
      Ternary(propertyKey.getOptId(state.query).exists(state.query.nodeOps.hasProperty(pc, _)))
    case pc: Relationship =>
      Ternary(propertyKey.getOptId(state.query).exists(state.query.relationshipOps.hasProperty(pc, _)))
    case null     => IsFalse
    case NotBound => NotApplicable
    case _        => throw new CypherTypeException("Expected " + identifier + " to be a property container.")
  }

  override def toString: String = "hasProp(" + propertyKey.name + ")"

  def containsIsNull = false

  def rewrite(f: (Expression) => Expression) = Has(identifier.rewrite(f), propertyKey.rewrite(f))

  def children = Seq(identifier)

  def assertInnerTypes(symbols: SymbolTable) {
    identifier.evaluateType(MapType(), symbols)
  }

  def symbolTableDependencies = identifier.symbolTableDependencies
}

case class LiteralRegularExpression(a: Expression, regex: Literal) extends Predicate {
  lazy val pattern = regex.v.asInstanceOf[String].r.pattern
  
  def isMatch(m: ExecutionContext)(implicit state: QueryState) =
    pattern.matcher(a(m).asInstanceOf[String]).matches()

  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = regex.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other => RegularExpression(a.rewrite(f), other)
  }

  def children = Seq(a, regex)

  def assertInnerTypes(symbols: SymbolTable) {
    a.evaluateType(StringType(), symbols)
    regex.evaluateType(StringType(), symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class RegularExpression(a: Expression, regex: Expression) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = {
    val value = a(m).asInstanceOf[String]
    val regularExp = regex(m).asInstanceOf[String]

    regularExp.r.pattern.matcher(value).matches()
  }

  override def toString(): String = a.toString() + " ~= /" + regex.toString() + "/"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = regex.rewrite(f) match {
    case lit:Literal => LiteralRegularExpression(a.rewrite(f), lit)
    case other => RegularExpression(a.rewrite(f), other)
  }

  def children = Seq(a, regex)

  def assertInnerTypes(symbols: SymbolTable) {
    a.evaluateType(StringType(), symbols)
    regex.evaluateType(StringType(), symbols)
  }
  def symbolTableDependencies = a.symbolTableDependencies ++ regex.symbolTableDependencies
}

case class NonEmpty(collection:Expression) extends Predicate with CollectionSupport {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = {
    collection(m) match {
      case IsCollection(x) => this.makeTraversable(collection(m)).nonEmpty
      case null            => false
      case x               => throw new CypherTypeException("Expected a collection, got `%s`".format(x))
    }
  }

  override def toString(): String = "nonEmpty(" + collection.toString() + ")"
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = NonEmpty(collection.rewrite(f))
  def children = Seq(collection)
  def assertInnerTypes(symbols: SymbolTable) {
    collection.evaluateType(CollectionType(AnyType()), symbols)
  }

  def symbolTableDependencies = collection.symbolTableDependencies
}

case class HasLabel(entity: Expression, label: KeyToken)  extends TernaryPredicate with CollectionSupport {

  override def ternaryIsMatch(m: ExecutionContext)(implicit state: QueryState): Ternary = entity(m) match {

    case NotBound =>
      NotApplicable

    case null =>
      IsFalse

    case value =>
      val node           = CastSupport.erasureCastOrFail[Node](value)
      val nodeId         = node.getId
      val queryCtx       = state.query

      val labelId = try {
        label.getOrCreateId(state.query)
      } catch {
        // If we are running in a query were we can't write changes,
        // just return false for this predicate.
        case _: NotInTransactionException => return IsFalse
      }
      Ternary(queryCtx.isLabelSetOnNode(labelId, nodeId))
  }

  override def toString = s"hasLabel(${entity}: ${label.name})"

  def rewrite(f: (Expression) => Expression) = HasLabel(entity.rewrite(f), label.typedRewrite[KeyToken](f))

  def children = Seq(label, entity)

  def symbolTableDependencies = entity.symbolTableDependencies ++ label.symbolTableDependencies

  def assertInnerTypes(symbols: SymbolTable) {
    entity.throwIfSymbolsMissing(symbols)
    label.throwIfSymbolsMissing(symbols)
    entity.evaluateType(NodeType(), symbols)
  }

  def containsIsNull = false
}
