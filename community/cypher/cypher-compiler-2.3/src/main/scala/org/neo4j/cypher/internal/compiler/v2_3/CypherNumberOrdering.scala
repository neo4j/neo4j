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
package org.neo4j.cypher.internal.compiler.v2_3

import java.lang

object CypherNumberOrdering extends Ordering[Number] {

  override def compare(x: Number, y: Number) = {
    if (areComparableOfSameType(x, y))
      compareValuesOfSameType(x, y)
    else
      compareValuesOfDifferentType(x, y)
  }

  private def areComparableOfSameType(l: Number, r: Number): Boolean =
    l.isInstanceOf[Comparable[_]] && l.getClass.isInstance(r)

  private def compareValuesOfSameType(l: Number, r: Number): Int =
    l.asInstanceOf[Comparable[Number]].compareTo(r)

  private def compareValuesOfDifferentType(x: Number, y: Number): Int = {
    (x, y) match {
      case (l: lang.Double, r: Number) => java.lang.Double.compare(l.doubleValue(), r.doubleValue())
      case (l: Number, r: lang.Double) => java.lang.Double.compare(l.doubleValue(), r.doubleValue())

      case (l: lang.Float, r: Number) => java.lang.Float.compare(l.floatValue(), r.floatValue())
      case (l: Number, r: lang.Float) => java.lang.Float.compare(l.floatValue(), r.floatValue())

      case (l, r) => java.lang.Long.compare(l.longValue(), r.longValue())
    }
  }
}
