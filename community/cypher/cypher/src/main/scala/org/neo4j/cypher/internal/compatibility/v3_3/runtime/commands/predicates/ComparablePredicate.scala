/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{Expression, Literal, Variable}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.values.AnyValues
import org.neo4j.values.storable._
import org.neo4j.values.virtual.{ListValue, VirtualValues}

abstract sealed class ComparablePredicate(val left: Expression, val right: Expression) extends Predicate {

  def compare(comparisonResult: Int): Boolean

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    val l = left(m)
    val r = right(m)

    if (l == Values.NO_VALUE || r == Values.NO_VALUE) None
    else (l, r) match {
      case (d: FloatingPointValue, _) if d.doubleValue().isNaN => None
      case (_, d: FloatingPointValue) if d.doubleValue().isNaN => None
      case (n1: NumberValue, n2: NumberValue) => Some(compare(AnyValues.COMPARATOR.compare(n1, n2)))
      case (n1: TextValue, n2: TextValue) => Some(compare(AnyValues.COMPARATOR.compare(n1, n2)))
      case (n1: BooleanValue, n2: BooleanValue) => Some(compare(AnyValues.COMPARATOR.compare(n1, n2)))
      case _ => None
    }
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

case class Equals(a: Expression, b: Expression) extends Predicate {

  def other(x: Expression): Option[Expression] = {
    if (x == a) Some(b)
    else if (x == b) Some(a)
    else None
  }

  def isMatch(m: ExecutionContext)(implicit state: QueryState): Option[Boolean] = {
    val a1 = a(m)
    val b1 = b(m)

    (a1, b1) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => None
      case (x: ArrayValue, y: ListValue) => Some(VirtualValues.fromArray(x).equals(y))
      case (x: ListValue, y: ArrayValue) => Some(VirtualValues.fromArray(y).equals(x))
      case _ => Some(a1.equals(b1))
    }
  }

  override def toString = s"$a == $b"

  def containsIsNull = (a, b) match {
    case (Variable(_), Literal(null)) => true
    case _ => false
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
