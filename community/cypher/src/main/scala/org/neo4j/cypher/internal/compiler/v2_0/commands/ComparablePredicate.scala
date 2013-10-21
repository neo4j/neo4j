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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import org.neo4j.cypher.internal.compiler.v2_0._
import expressions.{Identifier, Literal, Expression}
import pipes.QueryState
import symbols._
import org.neo4j.cypher.internal.helpers.IsCollection

abstract sealed class ComparablePredicate(left: Expression, right: Expression) extends Predicate with Comparer {
  def compare(comparisonResult: Int): Boolean

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = {
    val l: Any = left(m)
    val r: Any = right(m)

    if (l == null && r == null)
      return compare(0)
    
    if(l == null || r == null)
      return false

    val comparisonResult: Int = compare(l, r)(state)

    compare(comparisonResult)
  }

  def sign: String
  override def toString = left.toString() + " " + sign + " " + right.toString()
  def containsIsNull = false

  def children = Seq(left, right)

  def assertInnerTypes(symbols: SymbolTable) {
    left.throwIfSymbolsMissing(symbols)
    right.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = left.symbolTableDependencies ++ right.symbolTableDependencies
}

case class Equals(a: Expression, b: Expression) extends Predicate with Comparer {
  def other(x:Expression):Option[Expression] = {
    if      (x == a) Some(b)
    else if (x == b) Some(a)
    else             None
  }

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Boolean = {
    val a1 = a(m)
    val b1 = b(m)

    val result = (a1, b1) match {
      case (IsCollection(l), IsCollection(r)) => l == r
      case _                                  => a1 == b1
    }
    result
  }

  override def toString = a.toString() + " == " + b.toString()

  def containsIsNull = (a, b) match {
    case (Identifier(_), Literal(null)) => true
    case _                              => false
  }

  def rewrite(f: (Expression) => Expression) = Equals(a.rewrite(f), b.rewrite(f))

  def children = Seq(a, b)

  def assertInnerTypes(symbols: SymbolTable) {
    a.throwIfSymbolsMissing(symbols)
    b.throwIfSymbolsMissing(symbols)
  }

  def symbolTableDependencies = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class LessThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult < 0
  def sign: String = "<"
  def rewrite(f: (Expression) => Expression) = LessThan(a.rewrite(f), b.rewrite(f))
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult > 0
  def sign: String = ">"
  def rewrite(f: (Expression) => Expression) = GreaterThan(a.rewrite(f), b.rewrite(f))
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult <= 0
  def sign: String = "<="
  def rewrite(f: (Expression) => Expression) = LessThanOrEqual(a.rewrite(f), b.rewrite(f))
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult >= 0
  def sign: String = ">="
  def rewrite(f: (Expression) => Expression) = GreaterThanOrEqual(a.rewrite(f), b.rewrite(f))
}
