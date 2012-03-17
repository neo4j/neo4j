/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import collection.Seq
import org.neo4j.cypher.internal.Comparer
import java.lang.String
import org.neo4j.cypher.internal.symbols.{AnyType, ScalarType, Identifier}
import collection.Map

abstract sealed class ComparablePredicate(left: Expression, right: Expression) extends Predicate with Comparer {
  def compare(comparisonResult: Int): Boolean

  def isMatch(m: Map[String, Any]): Boolean = {
    val l: Any = left(m)
    val r: Any = right(m)

    val comparisonResult: Int = compare(l, r)

    compare(comparisonResult)
  }

  def dependencies: Seq[Identifier] = left.dependencies(ScalarType()) ++ right.dependencies(ScalarType())
  def sign: String
  def atoms: Seq[Predicate] = Seq(this)
  override def toString = left.toString() + " " + sign + " " + right.toString()
  def exists(f: (Expression) => Boolean) = left.exists(f) || right.exists(f)
  def containsIsNull = false
  def filter(f: (Expression) => Boolean): Seq[Expression] = left.filter(f) ++ right.filter(f)
}

case class Equals(a: Expression, b: Expression) extends Predicate with Comparer {
  def isMatch(m: Map[String, Any]): Boolean = {
    val a1 = a(m)
    val b1 = b(m)
    a1 == b1
  }
  def atoms = Seq(this)
  def exists(f: (Expression) => Boolean) = a.exists(f) || b.exists(f)
  def dependencies = a.dependencies(AnyType()) ++ b.dependencies(AnyType())
  override def toString = a.toString() + " == " + b.toString()
  def containsIsNull = false
  def rewrite(f: (Expression) => Expression) = Equals(a.rewrite(f), b.rewrite(f))
  def filter(f: (Expression) => Boolean): Seq[Expression] = a.filter(f) ++ b.filter(f)
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