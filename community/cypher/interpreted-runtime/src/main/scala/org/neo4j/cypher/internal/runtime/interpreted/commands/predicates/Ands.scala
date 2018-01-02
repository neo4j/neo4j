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
package org.neo4j.cypher.internal.runtime.interpreted.commands.predicates

import org.neo4j.cypher.internal.util.v3_4.NonEmptyList
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Property, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState

case class Ands(predicates: NonEmptyList[Predicate]) extends CompositeBooleanPredicate {
  override def shouldExitWhen = false
  override def andWith(other: Predicate): Predicate = Ands(predicates :+ other)
  override def rewrite(f: (Expression) => Expression): Expression = f(Ands(predicates.map(_.rewriteAsPredicate(f))))
  override def toString = {
    predicates.foldLeft("") {
      case (acc, next) if acc.isEmpty => next.toString
      case (acc, next) => s"$acc AND $next"
    }
  }
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
  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = Ands(NonEmptyList(a, b)).isMatch(m, state)

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

case class AndedPropertyComparablePredicates(ident: Variable, prop: Property,
                                             override val predicates: NonEmptyList[ComparablePredicate])
  extends CompositeBooleanPredicate {

  // some rewriters change the type of this, and we can't allow that
  private def rewriteVariableIfNotTypeChanged(f: (Expression) => Expression) =
    ident.rewrite(f) match {
      case i: Variable => i
      case _ => ident
    }

  def rewrite(f: (Expression) => Expression): Expression =
    f(AndedPropertyComparablePredicates(rewriteVariableIfNotTypeChanged(f),
      prop.rewrite(f).asInstanceOf[Property],
      predicates.map(_.rewriteAsPredicate(f).asInstanceOf[ComparablePredicate])))

  override def shouldExitWhen: Boolean = false
}
