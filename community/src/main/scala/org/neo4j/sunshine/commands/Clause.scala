/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.sunshine.commands

import org.neo4j.graphdb.PropertyContainer
import scala.AnyRef

/**
 * Created by Andres Taylor
 * Date: 4/16/11
 * Time: 13:29 
 */


abstract sealed class Clause {
  def ++(other: Clause): Clause = And(this, other)

  def isMatch(m: Map[String, Any]): Boolean
}

case class And(a: Clause, b: Clause) extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) && b.isMatch(m)
}

case class Equals(a: Value, b: Value) extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = a.value(m) == b.value(m)
}

case class Or(a: Clause, b: Clause) extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = a.isMatch(m) || b.isMatch(m)
}

case class Not(a: Clause) extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = !a.isMatch(m)
}

case class True() extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = true
}

case class RegularExpression(a:Value, str:String) extends Clause {
  def isMatch(m: Map[String, Any]): Boolean = {
    val value = a.value(m).asInstanceOf[String]
    str.r.pattern.matcher(value).matches()
  }
}


abstract sealed class ComparableClause(a: Value, b: Value) extends Clause {
  def isOrderedMatch(a: Comparable[AnyRef], b: Comparable[AnyRef]): Boolean

  def isMatch(m: Map[String, Any]): Boolean = (a.value(m), b.value(m)) match {
    case x: (Comparable[AnyRef], Comparable[AnyRef]) => isOrderedMatch(x._1, x._2)
    case _ => throw new RuntimeException
  }
}

case class LessThan(a: Value, b: Value) extends ComparableClause(a, b) {
  def isOrderedMatch(a: Comparable[AnyRef], b: Comparable[AnyRef]) = a.compareTo(b) <  0
}

case class GreaterThan(a: Value, b: Value) extends ComparableClause(a, b) {
  def isOrderedMatch(a: Comparable[AnyRef], b: Comparable[AnyRef]) = a.compareTo(b) > 0
}

case class LessThanOrEqual(a: Value, b: Value) extends ComparableClause(a, b) {
  def isOrderedMatch(a: Comparable[AnyRef], b: Comparable[AnyRef]) = a.compareTo(b) <=  0
}

case class GreaterThanOrEqual(a: Value, b: Value) extends ComparableClause(a, b) {
  def isOrderedMatch(a: Comparable[AnyRef], b: Comparable[AnyRef]) = a.compareTo(b) >= 0
}

