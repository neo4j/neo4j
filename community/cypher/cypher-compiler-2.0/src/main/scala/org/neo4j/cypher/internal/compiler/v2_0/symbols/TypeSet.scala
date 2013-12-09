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
  def apply(types: CypherType*): TypeSet = apply(types)
  def apply[T <: CypherType](traversable: TraversableOnce[T]): TypeSet = RangedTypeSet(traversable.map(t => TypeRange(t, t)))

  val empty: TypeSet = RangedTypeSet()
  val all: TypeSet = RangedTypeSet(TypeRange(AnyType(), None))
  val allNumbers: TypeSet = RangedTypeSet(TypeRange(NumberType(), None))

  private val simpleTypes = Seq(
    AnyType(),
    BooleanType(),
    DoubleType(),
    IntegerType(),
    LongType(),
    MapType(),
    NodeType(),
    NumberType(),
    PathType(),
    RelationshipType(),
    StringType()
  )

  object RangedTypeSet {
    def apply(): RangedTypeSet = new RangedTypeSet(Seq.empty)
    def apply(range: TypeRange): RangedTypeSet = new RangedTypeSet(Seq(range))
    def apply(ranges: TraversableOnce[TypeRange]): RangedTypeSet = new RangedTypeSet(ranges.foldLeft(Seq.empty[TypeRange]) {
      case (set, range) =>
        if (set.exists(_ contains range))
          set
        else
          set.filterNot(range contains) :+ range
    })
  }

  class RangedTypeSet private (val ranges: Seq[TypeRange]) extends TypeSet {
    def contains(aType: CypherType): Boolean = contains(aType, ranges)
    private def contains(aType: CypherType, rs: Seq[TypeRange]): Boolean = rs.exists(_ contains aType)

    def ++(that: TypeSet): RangedTypeSet = that match {
      case rts: RangedTypeSet => RangedTypeSet(ranges ++ rts.ranges)
    }

    def intersect(that: TypeSet): RangedTypeSet = that match {
      case rts: RangedTypeSet => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r intersect)
      })
    }

    def constrain(other: TypeSet): RangedTypeSet = other match {
      case rts: RangedTypeSet => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r constrain _.lower)
      })
    }

    def mergeDown(other: TypeSet): RangedTypeSet = other match {
      case rts: RangedTypeSet => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r mergeDown)
      })
    }

    def +(elem: CypherType): RangedTypeSet = RangedTypeSet(ranges :+ TypeRange(elem, elem))

    override def reparent(f: CypherType => CypherType): RangedTypeSet = RangedTypeSet(ranges.map(_.reparent(f)))

    def isEmpty: Boolean = ranges.isEmpty
    def nonEmpty: Boolean = !isEmpty

    def hasDefiniteSize: Boolean = _hasDefiniteSize
    private lazy val _hasDefiniteSize = ranges.forall(_.hasDefiniteSize)

    def toStream: Stream[CypherType] = toStream(ranges)
    private def toStream(rs: Seq[TypeRange]): Stream[CypherType] =
      if (rs.isEmpty) Stream()
      else
        simpleTypes.filter(contains(_, rs)).toStream append (toStream(innerTypeRanges(rs)).map(t => CollectionType(t): CypherType): Stream[CypherType])

    def iterator: Iterator[CypherType] = toStream.iterator

    override def hashCode = 41 * ranges.hashCode
    override def equals(that: Any): Boolean = that match {
      case that: RangedTypeSet =>
        (that canEqual this) && {
          val (finite1, infinite1) = ranges.partition(_.hasDefiniteSize)
          val (finite2, infinite2) = that.ranges.partition(_.hasDefiniteSize)
          (infinite1 == infinite2) &&
            ((finite1 == finite2) || (RangedTypeSet(finite1).toStream == RangedTypeSet(finite2).toStream))
        }
      case _                   => false
    }
    override def canEqual(other: Any): Boolean = other.isInstanceOf[TypeSet]

    def toStrings: IndexedSeq[String] = toStrings(ranges)

    private def toStrings(rs: Seq[TypeRange]): IndexedSeq[String] =
      if (rs.isEmpty) Vector()
      else if (rs.exists({ case TypeRange(_: AnyType, None) => true case _ => false })) Vector("<T>")
      else simpleTypes.filter(contains(_, rs)).map(_.toString).toIndexedSeq ++ toStrings(innerTypeRanges(rs)).map(t => s"Collection<$t>")

    private def innerTypeRanges: Seq[TypeRange] => Seq[TypeRange] = _.flatMap {
      case TypeRange(c: CollectionType, Some(u: CollectionType)) => Some(TypeRange(c.innerType, u.innerType))
      case TypeRange(c: CollectionType, None)                    => Some(TypeRange(c.innerType, None))
      case TypeRange(_: AnyType, Some(u: CollectionType))        => Some(TypeRange(AnyType(), u.innerType))
      case TypeRange(_: AnyType, None)                           => Some(TypeRange(AnyType(), None))
      case _                                                     => None
    }
  }
}


sealed trait TypeSet extends Any with Equals {
  def contains(elem: CypherType): Boolean

  def +(elem: CypherType): TypeSet

  def ++(that: TypeSet): TypeSet

  def intersect(that: TypeSet): TypeSet
  def &(that: TypeSet): TypeSet = this intersect that

  def constrain(types: CypherType*): TypeSet = constrain(TypeSet(types:_*))
  def constrain(types: TypeSet): TypeSet
  def mergeDown(types: CypherType*): TypeSet = mergeDown(TypeSet(types:_*))
  def mergeDown(other: TypeSet): TypeSet

  def reparent(f: CypherType => CypherType): TypeSet

  def isEmpty: Boolean
  def nonEmpty: Boolean

  def hasDefiniteSize: Boolean

  def iterator: Iterator[CypherType]

  def toStrings: IndexedSeq[String]

  def mkString(sep: String): String =
    mkString("", sep, sep, "")
  def mkString(sep: String, lastSep: String): String =
    mkString("", sep, lastSep, "")
  def mkString(start: String, sep: String, end: String): String =
    mkString(start, sep, sep, end)
  def mkString(start: String, sep: String, lastSep: String, end: String): String =
    addString(new StringBuilder(), start, sep, lastSep, end).toString()

  def addString(b: StringBuilder, start: String, sep: String, lastSep: String, end: String): StringBuilder = {
    val strings = toStrings
    if (strings.length > 1)
      strings.dropRight(1).addString(b, start, sep, "").append(lastSep).append(strings.last).append(end)
    else
      strings.addString(b, start, sep, end)
  }

  override def toString = mkString("TypeSet(", ", ", ")")
}
