/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.{AnyValues, Comparison}
import org.neo4j.values.storable._

abstract sealed class ComparablePredicate(val left: Expression, val right: Expression) extends Predicate {

  def compare(comparisonResult: Option[Int]): Option[Boolean]

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = left(m, state)
    val r = right(m, state)

    val res = if (l == Values.NO_VALUE || r == Values.NO_VALUE) None
    else (l, r) match {
      case (d: FloatingPointValue, _) if d.doubleValue().isNaN => None
      case (_, d: FloatingPointValue) if d.doubleValue().isNaN => None
      case (n1: NumberValue, n2: NumberValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: TextValue, n2: TextValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: BooleanValue, n2: BooleanValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: PointValue, n2: PointValue) => this match {
          // The ternary comparator cannot handle the '='  part of the >= and <= cases, so we need to switch to the within function
        case _: LessThanOrEqual => asOption(n1.withinRange(null, false, n2, true))
        case _: GreaterThanOrEqual => asOption(n1.withinRange(n2, true, null, false))
        case _: LessThan => asOption(n1.withinRange(null, false, n2, false))
        case _: GreaterThan => asOption(n1.withinRange(n2, false, null, false))
        case _ => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      }
      case (n1: DateValue, n2: DateValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: LocalTimeValue, n2: LocalTimeValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: TimeValue, n2: TimeValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: LocalDateTimeValue, n2: LocalDateTimeValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case (n1: DateTimeValue, n2: DateTimeValue) => compare(undefinedToNone(AnyValues.TERNARY_COMPARATOR.ternaryCompare(n1, n2)))
      case _ => None
    }
    res
  }

  private def asOption(result: Any): Option[Boolean] = result match {
    case true => Some(true)
    case false => Some(false)
    case _ => None
  }

  private def undefinedToNone(comparison: Comparison): Option[Int] = comparison match {
    case Comparison.UNDEFINED => None
    case Comparison.GREATER_THAN_AND_EQUAL => None
    case Comparison.SMALLER_THAN_AND_EQUAL => None
    case _ => Some(comparison.value())
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

  def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = a(m, state)
    val r = b(m, state)

    l.ternaryEquals(r) match {
      case null => None
      case v => Some(v)
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

  override def compare(comparisonResult: Option[Int]): Option[Boolean] = comparisonResult.map(_ < 0)

  def sign: String = "<"

  def rewrite(f: (Expression) => Expression) = f(LessThan(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def compare(comparisonResult: Option[Int]): Option[Boolean] = comparisonResult.map(_ > 0)

  def sign: String = ">"

  def rewrite(f: (Expression) => Expression) = f(GreaterThan(a.rewrite(f), b.rewrite(f)))
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def compare(comparisonResult: Option[Int]): Option[Boolean] = comparisonResult.map(_ <= 0)

  def sign: String = "<="

  def rewrite(f: (Expression) => Expression) = f(LessThanOrEqual(a.rewrite(f), b.rewrite(f)))
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def compare(comparisonResult: Option[Int]): Option[Boolean] = comparisonResult.map(_ >= 0)

  def sign: String = ">="

  def rewrite(f: (Expression) => Expression) = f(GreaterThanOrEqual(a.rewrite(f), b.rewrite(f)))
}
