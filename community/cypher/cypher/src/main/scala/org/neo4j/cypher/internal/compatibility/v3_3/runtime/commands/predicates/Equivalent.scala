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

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{CRS, GeographicPoint}
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

// Class that calculates if two values are equivalent or not.
// Does not handle NULL values - that must be handled outside!
class Equivalent(protected val eagerizedValue: Any, val originalValue: Any) extends scala.Equals {
  override def equals(in: Any): Boolean = {
    val eagerOther = in match {
      case s: Equivalent =>
        if(originalValue.isInstanceOf[AnyRef] &&
          s.originalValue.isInstanceOf[AnyRef] &&
          (originalValue.asInstanceOf[AnyRef] eq s.originalValue.asInstanceOf[AnyRef])) return true
        s.eagerizedValue
      case x =>
        if(originalValue.isInstanceOf[AnyRef] &&
          x.isInstanceOf[AnyRef] &&
          (originalValue.asInstanceOf[AnyRef] eq x.asInstanceOf[AnyRef])) return true
        Equivalent.eager(x)
    }

    (eagerizedValue, eagerOther) match {
      case (null, null) => true
      case (n1: Double, n2: Float) => mixedFloatEquality(n2, n1)
      case (n1: Float, n2: Double) => mixedFloatEquality(n1, n2)
      case (a, b) => a == b
      case _ => false
    }
  }

  override def canEqual(that: Any): Boolean = true

  private def mixedFloatEquality(a: Float, b: Double) = a.toDouble == b

  private var hash: Option[Int] = None

  override def hashCode(): Int = hash.getOrElse {
    val result = hashCode(eagerizedValue)
    hash = Some(result)
    result
  }

  private val EMPTY_LIST = 42

  private def hashCode(o: Any): Int = o match {
    case null => 0
    case n: Number => n.longValue().hashCode()
    case n: Tuple1[Any] => hashCode(n._1)
    case (n1, n2) => 31 * hashCode(n1) + hashCode(n2)
    case (n1, n2, n3) => (31 * hashCode(n1) + hashCode(n2)) * 31 + hashCode(n3)
    case n: IndexedSeq[_] =>
      val length = n.length
      if (length > 0)
        length * (31 * hashCode(n.head) + hashCode(n(length / 2)) * 31 + hashCode(n.last))
      else
        EMPTY_LIST
    case x => x.hashCode()
  }
}

object Equivalent {
  def apply(x: Any): Equivalent = new Equivalent(eager(x), x)

  private def eager(v: Any): Any = v match {
    case x: Number => x
    case a: String => a
    case n: Node => n
    case n: Relationship => n
    case p: Path => p
    case b: Boolean => b
    case null => null
    case a: Char => a.toString

    case a: Array[_] => wrapEager(a)
    case m: java.util.Map[_, _] => m.asScala.mapValues(eager)
    case m: Map[_, _] => m.mapValues(eager)
    case a: TraversableOnce[_] => wrapEager(a)
    case l: java.lang.Iterable[_] => wrapEager(l.asScala)
    case l: GeographicPoint => l
    case x: org.neo4j.graphdb.spatial.Point =>
      val crs = CRS.fromURL(x.getCRS.getHref)
      val coordinates = x.getCoordinate.getCoordinate.asScala
      GeographicPoint(coordinates.head, coordinates.last, crs)

    case x => throw new IllegalStateException(s"unknown value: ($x) of type ${x.getClass})")
  }

  private def wrapEager[T](x: GenTraversableOnce[T]): Any = {
    val it = x.toIterator
    if (it.isEmpty) return None
    val e1 = eager(it.next())
    if (it.isEmpty) return e1
    val e2 = eager(it.next())
    if (it.isEmpty) return (e1, e2)
    val e3 = eager(it.next())
    if (it.isEmpty) return (e1, e2, e3)

    it.foldLeft(ArrayBuffer(e1, e2, e3)) {
      case (acc, element) => acc += eager(element)
    }
  }
}
