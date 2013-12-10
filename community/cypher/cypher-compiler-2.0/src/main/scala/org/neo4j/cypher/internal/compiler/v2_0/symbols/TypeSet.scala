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

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder

object TypeSet {
  implicit def canBuildFrom = new CanBuildFrom[Iterable[CypherType], CypherType, TypeSet] {
    def apply(from: Iterable[CypherType]): Builder[CypherType, TypeSet] = this.apply()
    def apply(): Builder[CypherType, TypeSet] = new Builder[CypherType, TypeSet] {
      protected var elems: Set[CypherType] = Set.empty
      def +=(elem: CypherType): this.type = { elems = elems + elem; this }
      def clear() { elems = Set.empty }
      def result(): TypeSet = TypeSet(elems)
    }
  }

  def apply(types: CypherType*): TypeSet = new DiscreteTypeSet(types.toSet)
  def apply[T <: CypherType](traversable: TraversableOnce[T]): TypeSet = new DiscreteTypeSet(traversable.toSet)

  val empty: TypeSet = new DiscreteTypeSet(Set.empty[CypherType])

  case class DiscreteTypeSet(set: Set[CypherType]) extends TypeSet {
    def contains(elem: CypherType): Boolean = set.contains(elem)

    def +(elem: CypherType): TypeSet = copy(set.+(elem))

    def ++(that: TypeSet): TypeSet = that match {
      case dts: DiscreteTypeSet => copy(set ++ dts.set)
    }

    def intersect(that: TypeSet): TypeSet = that match {
      case dts: DiscreteTypeSet => copy(set & dts.set)
    }

    def constrain(other: TypeSet): TypeSet = copy(set.filter {
      t => other.exists(_.isAssignableFrom(t))
    })

    def mergeDown(other: TypeSet): TypeSet = set.flatMap {
      t => other.map(_ mergeDown t)
    }

    def mergeUp(other: TypeSet): TypeSet = set.flatMap {
      t => other.flatMap(_ mergeUp t)
    }

    def iterator: Iterator[CypherType] = set.iterator

    override def equals(that: Any) = that match {
      case dts: DiscreteTypeSet => (dts canEqual this) && set.equals(dts.set)
      case _                    => false
    }

    def toStrings: IndexedSeq[String] = set.map(_.toString).toIndexedSeq.sorted

    override def stringPrefix: String = "DiscreteTypeSet"
  }
}


sealed trait TypeSet extends Iterable[CypherType] {
  def contains(elem: CypherType): Boolean

  def +(elem: CypherType): TypeSet

  def ++(that: TypeSet): TypeSet

  def intersect(that: TypeSet): TypeSet
  def &(that: TypeSet): TypeSet = this intersect that

  def constrain(types: CypherType*): TypeSet = constrain(TypeSet(types:_*))
  def constrain(types: TypeSet): TypeSet
  def mergeDown(types: CypherType*): TypeSet = mergeDown(TypeSet(types:_*))
  def mergeDown(other: TypeSet): TypeSet
  def mergeUp(other: TypeSet): TypeSet

  def reparent(f: CypherType => CypherType): TypeSet = map(f)

  def toStrings: IndexedSeq[String]

  def mkString(sep: String, lastSep: String): String =
    mkString("", sep, lastSep, "")
  def mkString(start: String, sep: String, lastSep: String, end: String): String =
    addString(new StringBuilder(), start, sep, lastSep, end).toString()

  override def addString(b: StringBuilder, start: String, sep: String, end: String): StringBuilder =
    addString(b, start, sep, sep, end)
  def addString(b: StringBuilder, start: String, sep: String, lastSep: String, end: String): StringBuilder = {
    val strings = toStrings
    if (strings.length > 1)
      strings.dropRight(1).addString(b, start, sep, "").append(lastSep).append(strings.last).append(end)
    else
      strings.addString(b, start, sep, end)
  }
}
