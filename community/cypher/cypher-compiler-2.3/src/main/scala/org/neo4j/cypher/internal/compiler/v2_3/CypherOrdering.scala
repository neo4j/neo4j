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

import java.util.Comparator

import org.neo4j.cypher.internal.compiler.v2_3.commands.values.forceInterpolation
import org.neo4j.kernel.impl.api.PropertyValueComparison

import MinMaxOrdering._

object CypherOrdering {
  val DEFAULT: Ordering[Any] =
    Ordering.comparatorToOrdering(PropertyValueComparison.COMPARE_VALUES.asInstanceOf[Comparator[Any]]).withForcedInterpolation.withNullsLast
}

case class MinMaxOrdering[T](ordering: Ordering[T]) {
  val forMin = ordering.withNullsFirst
  val forMax = ordering.withNullsLast
}

object MinMaxOrdering {
  val BY_VALUE: MinMaxOrdering[Any] =
    MinMaxOrdering(Ordering.comparatorToOrdering(PropertyValueComparison.COMPARE_VALUES.asInstanceOf[Comparator[Any]]).withForcedInterpolation)
  val BY_NUMBER: MinMaxOrdering[Number] =
    MinMaxOrdering(Ordering.comparatorToOrdering(PropertyValueComparison.COMPARE_NUMBERS))
  val BY_STRING: MinMaxOrdering[Any] =
    MinMaxOrdering(Ordering.comparatorToOrdering(PropertyValueComparison.COMPARE_STRINGS.asInstanceOf[Comparator[Any]]).withForcedInterpolation)

  implicit class NullOrdering[T](val ordering: Ordering[T]) extends AnyVal {

    def withNullsFirst = new Ordering[T] {
      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) 0 else -1
        } else if (y == null) {
          +1
        } else {
          ordering.compare(x, y)
        }
      }
    }

    def withNullsLast = new Ordering[T] {
      override def compare(x: T, y: T): Int = {
        if (x == null) {
          if (y == null) 0 else +1
        } else if (y == null) {
          -1
        } else {
          ordering.compare(x, y)
        }
      }
    }
  }

  implicit class AnyInterpolationOrdering(val ordering: Ordering[Any]) extends AnyVal {
    def withForcedInterpolation = new Ordering[Any] {
      override def compare(x: Any, y: Any): Int = ordering.compare(forceInterpolation(x), forceInterpolation(y))
    }
  }
}

