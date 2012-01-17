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
package org.neo4j.cypher.commands

import collection.Seq
import org.neo4j.cypher.internal.Comparer
import java.lang.String
import org.neo4j.cypher.symbols.{AnyType, ScalarType, Identifier}

// TODO: Allow comparison of nodes and rels
// This should be split into two - one for relative comparisons < > and so on, and one for equality comparisons = !=
// You should be able to compare two node-identifiers for equality, but not for gt lt
abstract sealed class ComparablePredicate(a: Expression, b: Expression) extends Predicate with Comparer {
  def compare(comparisonResult: Int): Boolean

  def isMatch(m: Map[String, Any]): Boolean = {
    val left: Any = a.apply(m)
    val right: Any = b.apply(m)

    if((a.isInstanceOf[Nullable] && left == null) ||
      (b.isInstanceOf[Nullable] && right == null))
      return true

    val comparisonResult: Int = compare(left, right)

    compare(comparisonResult)
  }

  def dependencies: Seq[Identifier] = a.dependencies(ScalarType()) ++ b.dependencies(ScalarType())

  def sign:String
  def atoms: Seq[Predicate] = Seq(this)
  override def toString = a.toString() + " " + sign + " " + b.toString()
  def containsIsNull: Boolean = false
}

case class Equals(a: Expression, b: Expression) extends Predicate with Comparer {
  def isMatch(m: Map[String, Any]): Boolean = {
    val left = a(m)
    val right = b(m)

    if((a.isInstanceOf[Nullable] && left == null) ||
      (b.isInstanceOf[Nullable] && right == null))
      true
    else
      left == right
  }

  def atoms = Seq(this)

  def containsIsNull = false

  def dependencies = a.dependencies(AnyType()) ++ b.dependencies(AnyType())

  override def toString = a.toString() + " == " + b.toString()
}

case class LessThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult < 0
  def sign: String = "<"
}

case class GreaterThan(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult > 0
  def sign: String = ">"
}

case class LessThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult <= 0
  def sign: String = "<="
}

case class GreaterThanOrEqual(a: Expression, b: Expression) extends ComparablePredicate(a, b) {
  def compare(comparisonResult: Int) = comparisonResult >= 0
  def sign: String = ">="
}