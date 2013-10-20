/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.symbols

object TypeSet {
  def apply(types: CypherType*): TypeSet = new Proxy(types.toSet)
  def apply[T <: CypherType](traversable: TraversableOnce[T]): TypeSet = new Proxy(traversable.toSet)

  def empty: TypeSet = new Proxy(Set.empty[CypherType])

  class Proxy(set: Set[CypherType]) extends TypeSet {
    def contains(elem: CypherType): Boolean = set.contains(elem)
    def +(elem: CypherType): TypeSet = new Proxy(set.+(elem))
    def -(elem: CypherType): TypeSet = new Proxy(set.-(elem))
    def iterator: Iterator[CypherType] = set.iterator
    override def equals(that: Any) = set.equals(that)
    override def hashCode(): Int = set.hashCode()
  }

}


trait TypeSet extends Set[CypherType] {

  def mergeDown(other: TypeSet): TypeSet = {
    TypeSet(foldLeft(Vector.empty[CypherType])((ts, t) => {
      val dt = other.mergeDown(t)
      ts.filter(_.mergeUp(dt) != Some(dt)) :+ dt
    }))
  }

  def mergeDown(other: CypherType): CypherType = {
    map {
      _.mergeDown(other)
    } reduce {
      (t1, t2) => (t1 mergeUp t2).get
    }
  }

  def mergeUp(other: TypeSet): TypeSet = {
    TypeSet(flatMap {
      t => other.flatMap {
        _ mergeUp t
      }
    })
  }

  def formattedString: String = {
    val types = toIndexedSeq.map(_.toString)
    types.length match {
      case 0 => ""
      case 1 => types.head
      case _ => s"${types.dropRight(1).mkString(", ")} or ${types.last}"
    }
  }
}
