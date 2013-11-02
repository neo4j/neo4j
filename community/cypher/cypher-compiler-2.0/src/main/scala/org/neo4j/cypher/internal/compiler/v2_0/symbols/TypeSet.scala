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
  def apply(types: CypherType*): TypeSet = new SetBackedTypeSet(types.toSet)
  def apply[T <: CypherType](traversable: TraversableOnce[T]): TypeSet = new SetBackedTypeSet(traversable.toSet)

  def empty: TypeSet = new SetBackedTypeSet(Set.empty[CypherType])

  class SetBackedTypeSet(set: Set[CypherType]) extends TypeSet {
    def contains(elem: CypherType): Boolean = set.contains(elem)
    def +(elem: CypherType): TypeSet = new SetBackedTypeSet(set.+(elem))
    def -(elem: CypherType): TypeSet = new SetBackedTypeSet(set.-(elem))
    def iterator: Iterator[CypherType] = set.iterator
    override def equals(that: Any) = set.equals(that)
    override def hashCode(): Int = set.hashCode()
  }
}


trait TypeSet extends Set[CypherType] {

  def constrain(other: TypeSet): TypeSet = {
    TypeSet(filter {
      t => other.exists(_.isAssignableFrom(t))
    })
  }

  def mergeDown(other: TypeSet): TypeSet = {
    TypeSet(flatMap {
      t => other.map(_ mergeDown t)
    })
  }

  def mergeUp(other: TypeSet): TypeSet = {
    TypeSet(flatMap {
      t => other.flatMap(_ mergeUp t)
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
