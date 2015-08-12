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
package org.neo4j.cypher.internal.compiler.v2_3.commands.predicates

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Property, Identifier, Expression}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState

case class Ands(predicates: List[Predicate]) extends CompositeBooleanPredicate {
  def shouldExitWhen = false
  override def andWith(other: Predicate): Predicate = Ands(predicates :+ other)
  def rewrite(f: (Expression) => Expression): Expression = f(Ands(predicates.map(_.rewriteAsPredicate(f))))
}

object Ands {
  def apply(a: Predicate, b: Predicate) = (a, b) match {
    case (True(), other) => other
    case (other, True()) => other
    case (_, _)          => new Ands(List(a, b))
  }
}

class And(val a: Predicate, val b: Predicate) extends Predicate {
  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = Ands(List(a, b)).isMatch(m)

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

object And {
  def apply(a: Predicate, b: Predicate) = (a, b) match {
    case (True(), other) => other
    case (other, True()) => other
    case (_, _)          => new And(a, b)
  }
}

case class AndedPropertyComparablePredicates(ident: Identifier, prop: Property, comparables: NonEmptyList[ComparablePredicate]) extends Predicate {

  def symbolTableDependencies: Set[String] = comparables.foldLeft(prop.symbolTableDependencies) {
    case (acc, predicate) => acc ++ predicate.symbolTableDependencies
  }

  def containsIsNull: Boolean = comparables.exists(_.containsIsNull)

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    var result: Option[Option[Boolean]] = None
    val iter = comparables.toIterable.iterator
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

  def arguments: Seq[Expression] = comparables.toSeq

  // some rewriters change the type of this, and we can't allow that
  private def rewriteIdentifierIfNotTypeChanged(f: (Expression) => Expression) =
    ident.rewrite(f) match {
      case i: Identifier => i
      case _ => ident
    }

  def rewrite(f: (Expression) => Expression): Expression =
    f(AndedPropertyComparablePredicates(rewriteIdentifierIfNotTypeChanged(f),
      prop.rewrite(f).asInstanceOf[Property],
      comparables.map(_.rewriteAsPredicate(f).asInstanceOf[ComparablePredicate])))

  override def atoms: Seq[Predicate] = comparables.toSeq
}
