/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.cypher.Comparer


abstract sealed class ComparableClause(a: Value, b: Value) extends Clause with Comparer {
  def compare(comparisonResult: Int): Boolean

  def isMatch(m: Map[String, Any]): Boolean = {
    val left: Any = a.apply(m)
    val right: Any = b.apply(m)

    if((a.isInstanceOf[NullablePropertyValue] && left == null) ||
      (b.isInstanceOf[NullablePropertyValue] && right == null))
      return true

    val comparisonResult: Int = compare(left, right)

    compare(comparisonResult)
  }

  def dependsOn: Set[String] = a.dependsOn ++ b.dependsOn
}

case class Equals(a: Value, b: Value) extends ComparableClause(a, b) {
  def compare(comparisonResult: Int) = comparisonResult == 0
}

case class LessThan(a: Value, b: Value) extends ComparableClause(a, b) {
  def compare(comparisonResult: Int) = comparisonResult < 0
}

case class GreaterThan(a: Value, b: Value) extends ComparableClause(a, b) {
  def compare(comparisonResult: Int) = comparisonResult > 0
}

case class LessThanOrEqual(a: Value, b: Value) extends ComparableClause(a, b) {
  def compare(comparisonResult: Int) = comparisonResult <= 0
}

case class GreaterThanOrEqual(a: Value, b: Value) extends ComparableClause(a, b) {
  def compare(comparisonResult: Int) = comparisonResult >= 0
}