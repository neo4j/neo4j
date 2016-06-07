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
package org.neo4j.cypher.internal.compiler.v3_1.commands.predicates

import org.neo4j.cypher.internal.compiler.v3_0.GeographicPoint
import org.neo4j.graphdb.{Node, Path, Relationship}

import collection.JavaConverters._

class Equivalent(val eagerizedValue: Any) {
  override def equals(in: Any): Boolean = {
    val eagerOther = in match {
      case s: Equivalent => s.eagerizedValue
      case x => Equivalent.eager(x)
    }

    (eagerizedValue, eagerOther) match {
      case (null, null) => true
      case (n1: Number, n2: Number) =>
        n1.doubleValue() == n2.doubleValue() ||
          (Math.rint(n1.doubleValue()).toLong == n2.longValue() &&
            Math.rint(n2.doubleValue()).toLong == n1.longValue())
      case (a, b) => a == b
      case _ => false
    }
  }

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
    case n: Vector[_] =>
      val length = n.length
      if (length > 0)
        length * (hashCode(n.head) + hashCode(n.last))
      else
        EMPTY_LIST
    case x => x.hashCode()
  }
}

object Equivalent {
  def apply(x: Any): Equivalent = new Equivalent(eager(x))

  private def eager(v: Any): Any = v match {
    case x: Number => x
    case a: String => a
    case n: Node => n
    case n: Relationship => n
    case p: Path => p
    case b: Boolean => b
    case null => null
    case a: Char => a.toString

    case a: Array[_] => a.toVector.map(eager)
    case m: java.util.Map[_,_] => m.asScala.mapValues(eager)
    case m: Map[_,_] => m.mapValues(eager)
    case a: TraversableOnce[_] => a.toVector.map(eager)
    case l: java.lang.Iterable[_] => l.asScala.toVector.map(eager)
    case l: GeographicPoint => l
    case x => throw new IllegalStateException(s"unknown value: (${x}) of type ${x.getClass})")
  }
}
