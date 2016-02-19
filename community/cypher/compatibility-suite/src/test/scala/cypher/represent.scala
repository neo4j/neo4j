/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher

import java.util

import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class represent(expected: util.List[util.Map[String, AnyRef]])
  extends Matcher[util.List[util.Map[String, AnyRef]]] {

  override def apply(actual: util.List[util.Map[String, AnyRef]]): MatchResult = {
    MatchResult(matches = compare(expected, actual), "a mismatch found", "no mismatches found")
  }

  def compare(expected: util.List[util.Map[String, AnyRef]], actual: util.List[util.Map[String, AnyRef]]): Boolean = {
    val expSorted = expected.asScala.toVector.map(_.asScala).sortBy(_.hashCode())
    val actSorted = actual.asScala.toVector.map(_.asScala).sortBy(_.hashCode())

    val bools = expSorted.zipWithIndex.map { case (expMap, index) =>
      val b = cypherEqual(expSorted(index), actSorted(index))
      if (!b)
        println(s"not equal: $expMap and ${actSorted(index)}")
      b
    }
    bools.reduce(_ && _)
  }

  def cypherEqual(expected: mutable.Map[String, AnyRef], actual: mutable.Map[String, AnyRef]) = {
    val keys = expected.keySet == actual.keySet
    val map = expected.keySet.map { key =>
      expected(key) == actual(key)
    }
    keys && map.reduce(_ && _)
  }

}
