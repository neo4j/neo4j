/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.predicates

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier, Property}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList

case class Ands(predicates: NonEmptyList[Predicate]) extends CompositeBooleanPredicate {
  def shouldExitWhen = false
  override def andWith(other: Predicate): Predicate = Ands(predicates :+ other)
  def rewrite(f: (Expression) => Expression): Expression = f(Ands(predicates.map(_.rewriteAsPredicate(f))))
}

object Ands {
  def apply(predicates: Predicate*) = predicates.filterNot(_ == True()).toList match {
    case Nil => True()
    case single :: Nil => single
    case manyPredicates => new Ands(NonEmptyList.from(manyPredicates))
  }
}

@deprecated("Use Ands (plural) instead")
class And(val a: Predicate, val b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = Ands(NonEmptyList(a, b)).isMatch(m)

  override def atoms: Seq[Predicate] = a.atoms ++ b.atoms
  override def toString: String = s"($a AND $b)"
  def containsIsNull = a.containsIsNull || b.containsIsNull
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

@deprecated("Use Ands (plural) instead")
object And {
  def apply(a: Predicate, b: Predicate) = (a, b) match {
    case (True(), other) => other
    case (other, True()) => other
    case (_, _)          => new And(a, b)
  }
}

case class AndedPropertyComparablePredicates(ident: Identifier, prop: Property,
                                             override val predicates: NonEmptyList[ComparablePredicate])
  extends CompositeBooleanPredicate {

  // some rewriters change the type of this, and we can't allow that
  private def rewriteIdentifierIfNotTypeChanged(f: (Expression) => Expression) =
    ident.rewrite(f) match {
      case i: Identifier => i
      case _ => ident
    }

  def rewrite(f: (Expression) => Expression): Expression =
    f(AndedPropertyComparablePredicates(rewriteIdentifierIfNotTypeChanged(f),
      prop.rewrite(f).asInstanceOf[Property],
      predicates.map(_.rewriteAsPredicate(f).asInstanceOf[ComparablePredicate])))

  override def shouldExitWhen: Boolean = false
}
