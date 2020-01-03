/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, Literal, Variable}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherBoolean
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

abstract sealed class ComparablePredicate(val left: Expression, val right: Expression) extends Predicate {

  def comparator: ((AnyValue, AnyValue) => Value)

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = left(m, state)
    val r = right(m, state)

    if (l == Values.NO_VALUE || r == Values.NO_VALUE) None
    else comparator(l, r) match {
      case Values.TRUE => Some(true)
      case Values.FALSE => Some(false)
      case Values.NO_VALUE => None
    }
  }

  def sign: String

  override def toString = left.toString() + " " + sign + " " + right.toString()

  override def containsIsNull = false

  override def arguments: Seq[Expression] = Seq(left, right)

  override def symbolTableDependencies: Set[String] = left.symbolTableDependencies ++ right.symbolTableDependencies

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

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val l = a(m, state)
    val r = b(m, state)

    l.ternaryEquals(r) match {
      case null => None
      case v => Some(v)
    }
  }

  override def toString = s"$a == $b"

  override def containsIsNull: Boolean = (a, b) match {
    case (Variable(_), Literal(null)) => true
    case _ => false
  }

  override def rewrite(f: Expression => Expression): Expression = f(Equals(a.rewrite(f), b.rewrite(f)))

  override def arguments: Seq[Expression] = Seq(a, b)

  override def children: Seq[AstNode[_]] = Seq(a, b)

  override def symbolTableDependencies: Set[String] = a.symbolTableDependencies ++ b.symbolTableDependencies
}

case class LessThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.lessThan

  override def sign: String = "<"

  override def rewrite(f: Expression => Expression): Expression = f(LessThan(a.rewrite(f), b.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(a, b)
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.greaterThan

  override def sign: String = ">"

  override def rewrite(f: Expression => Expression): Expression = f(GreaterThan(a.rewrite(f), b.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(a, b)
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.lessThanOrEqual

  override def sign: String = "<="

  override def rewrite(f: Expression => Expression): Expression = f(LessThanOrEqual(a.rewrite(f), b.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(a, b)
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {

  override def comparator: (AnyValue, AnyValue) => Value = CypherBoolean.greaterThanOrEqual

  override def sign: String = ">="

  override def rewrite(f: Expression => Expression): Expression = f(GreaterThanOrEqual(a.rewrite(f), b.rewrite(f)))

  override def children: Seq[AstNode[_]] = Seq(a, b)
}
