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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.IsCollection
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.IncomparableValuesException
import org.neo4j.graphdb.{Node, Relationship}

abstract sealed class ComparablePredicate(val left: Expression, val right: Expression) extends Predicate with Comparer {
  def compare(comparisonResult: Int): Boolean

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    val l: Any = left(m)
    val r: Any = right(m)

    if (l == null || r == null)
      return None

    val comparisonResult: Int = compare(None, l, r)(state)

    Some(compare(comparisonResult))
  }

  def sign: String
  override def toString = left.toString() + " " + sign + " " + right.toString()
  def containsIsNull = false

  def arguments = Seq(left, right)

  def symbolTableDependencies = left.symbolTableDependencies ++ right.symbolTableDependencies

  def other(e: Expression): Expression = if (e != left) {
    assert(e == right, "This expression is neither LHS nor RHS")
    left
  } else {
    right
  }
}

case class Equals(a: Expression, b: Expression) extends Predicate with Comparer {
  def other(x:Expression):Option[Expression] = {
    if      (x == a) Some(b)
    else if (x == b) Some(a)
    else             None
  }

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    val a1 = a(m)
    val b1 = b(m)

    (a1, b1) match {
      case (null, _)                                             => None
      case (_, null)                                             => None
      case (IsCollection(l), IsCollection(r))                    => Some(l == r)
      case (l: Node, r) if !r.isInstanceOf[Node]                 => incomparable(l, r)
      case (l, r: Node) if !l.isInstanceOf[Node]                 => incomparable(l, r)
      case (l: Relationship, r) if !r.isInstanceOf[Relationship] => incomparable(l, r)
      case (l, r: Relationship) if !l.isInstanceOf[Relationship] => incomparable(l, r)
      case (l: String, r: Character)                             => Some(l == r.toString)
      case (l: Character, r: String)                             => Some(l.toString == r)
      case _                                                     => Some(a1 == b1)
    }
  }

  private def incomparable(lhs: Any, rhs: Any)(implicit state: QueryState): Nothing =
    throw new IncomparableValuesException(textWithType(lhs), textWithType(rhs))

  override def toString = s"$a == $b"

  def containsIsNull = (a, b) match {
    case (Identifier(_), Literal(null)) => true
    case _                              => false
  }

  def rewrite(f: (Expression) => Expression) = f(Equals(a.rewrite(f), b.rewrite(f)))

  def arguments = Seq(a, b)

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class LessThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult < 0
  def sign: String = "<"
  def rewrite(f: (Expression) => Expression) = f(LessThan(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult > 0
  def sign: String = ">"
  def rewrite(f: (Expression) => Expression) = f(GreaterThan(a.rewrite(f), b.rewrite(f)))
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult <= 0
  def sign: String = "<="
  def rewrite(f: (Expression) => Expression) = f(LessThanOrEqual(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult >= 0
  def sign: String = ">="
  def rewrite(f: (Expression) => Expression) = f(GreaterThanOrEqual(a.rewrite(f), b.rewrite(f)))
}
