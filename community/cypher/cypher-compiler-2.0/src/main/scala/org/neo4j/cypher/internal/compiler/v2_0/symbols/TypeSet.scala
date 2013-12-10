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
  val all: TypeSet = RangedTypeSet(TypeRange(AnyType(), None))

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

  case class DiscreteTypeSet(set: Set[CypherType]) extends TypeSet {
    def contains(elem: CypherType): Boolean = set.contains(elem)

    def +(elem: CypherType): TypeSet = copy(set.+(elem))

    def ++(that: TypeSet): TypeSet = that match {
      case rts: RangedTypeSet   => rts ++ this
      case dts: DiscreteTypeSet => copy(set ++ dts.set)
    }

    def intersect(that: TypeSet): TypeSet = that match {
      case rts: RangedTypeSet   => rts & this
      case dts: DiscreteTypeSet => copy(set & dts.set)
    }

    def constrain(other: TypeSet): TypeSet = copy(set.filter {
      t => other.exists(_.isAssignableFrom(t))
    })

    def mergeDown(other: TypeSet): TypeSet = other match {
      case rts: RangedTypeSet =>
        rts mergeDown this
      case _                =>
        set.flatMap {
          t => other.map(_ mergeDown t)
        }
    }

    def iterator: Iterator[CypherType] = set.iterator

    override def equals(that: Any) = that match {
      case rts: RangedTypeSet   => rts.equals(this)
      case dts: DiscreteTypeSet => (dts canEqual this) && set.equals(dts.set)
      case _                    => false
    }

    def toStrings: IndexedSeq[String] = set.map(_.toString).toIndexedSeq.sorted

    override def stringPrefix: String = "DiscreteTypeSet"
  }

  object RangedTypeSet {
    def apply(range: TypeRange): RangedTypeSet = new RangedTypeSet(Seq(range))
    def apply(ranges: Seq[TypeRange]): RangedTypeSet = new RangedTypeSet(ranges.foldLeft(Seq.empty[TypeRange]) {
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

    def +(elem: CypherType): RangedTypeSet = RangedTypeSet(ranges :+ TypeRange(elem, elem))

    def ++(that: TypeSet): RangedTypeSet = that match {
      case dts: DiscreteTypeSet => RangedTypeSet(ranges ++ dts.set.map(t => TypeRange(t, t)))
      case rts: RangedTypeSet => RangedTypeSet(ranges ++ rts.ranges)
    }

    def intersect(that: TypeSet): RangedTypeSet = that match {
      case dts: DiscreteTypeSet => intersect(RangedTypeSet(dts.set.toSeq.map(t => TypeRange(t, t))))
      case rts: RangedTypeSet   => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r intersect)
      })
    }

    def constrain(other: TypeSet): RangedTypeSet = other match {
      case dts: DiscreteTypeSet => constrain(RangedTypeSet(dts.set.toSeq.map(TypeRange(_, None))))
      case rts: RangedTypeSet   => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r constrain _.lower)
      })
    }

    def mergeDown(other: TypeSet): RangedTypeSet = other match {
      case dts: DiscreteTypeSet => mergeDown(RangedTypeSet(dts.set.toSeq.map(t => TypeRange(t, t))))
      case rts: RangedTypeSet   => RangedTypeSet(ranges.flatMap {
        r => rts.ranges.flatMap(r mergeDown)
      })
    }

    override def reparent(f: CypherType => CypherType): RangedTypeSet = RangedTypeSet(ranges.map(_.reparent(f)))

    override def isEmpty: Boolean = ranges.isEmpty
    override def nonEmpty: Boolean = !isEmpty

    override def hasDefiniteSize: Boolean = _hasDefiniteSize
    private lazy val _hasDefiniteSize = ranges.forall(_.hasDefiniteSize)

    override def toStream: Stream[CypherType] = toStream(ranges)
    private def toStream(rs: Seq[TypeRange]): Stream[CypherType] =
      if (rs.isEmpty) Stream()
      else
        simpleTypes.filter(contains(_, rs)).toStream append (toStream(innerTypeRanges(rs)).map(t => CollectionType(t): CypherType): Stream[CypherType])

    def iterator: Iterator[CypherType] = toStream.iterator

    override def foreach[U](f: CypherType => U): Unit = {
      if (!hasDefiniteSize)
        throw new UnsupportedOperationException("Cannot map over indefinite collection")
      super.foreach(f)
    }

    override def hashCode = 41 * ranges.hashCode
    override def equals(that: Any): Boolean = that match {
      case that: RangedTypeSet  =>
        (that canEqual this) && {
          val (finite1, infinite1) = ranges.partition(_.hasDefiniteSize)
          val (finite2, infinite2) = that.ranges.partition(_.hasDefiniteSize)
          (infinite1 == infinite2) &&
            ((finite1 == finite2) || (RangedTypeSet(finite1).toStream == RangedTypeSet(finite2).toStream))
        }
      case dts: DiscreteTypeSet =>
        if (!hasDefiniteSize) false
        else TypeSet(this.iterator).equals(dts)
      case _                    => false
    }
    override def canEqual(other: Any): Boolean = other.isInstanceOf[TypeSet]

    def toStrings: IndexedSeq[String] = toStrings(ranges)

    private def toStrings(rs: Seq[TypeRange]): IndexedSeq[String] =
      if (rs.isEmpty) Vector()
      else if (rs.exists({ case TypeRange(_: AnyType, None) => true case _ => false })) Vector("T")
      else simpleTypes.filter(contains(_, rs)).map(_.toString).toIndexedSeq ++ toStrings(innerTypeRanges(rs)).map(t => s"Collection<$t>")

    override def stringPrefix: String = "RangedTypeSet"

    private def innerTypeRanges: Seq[TypeRange] => Seq[TypeRange] = _.flatMap {
      case TypeRange(c: CollectionType, Some(u: CollectionType)) => Some(TypeRange(c.innerType, u.innerType))
      case TypeRange(c: CollectionType, None)                    => Some(TypeRange(c.innerType, None))
      case TypeRange(_: AnyType, Some(u: CollectionType))        => Some(TypeRange(AnyType(), u.innerType))
      case r@TypeRange(_: AnyType, None)                         => Some(r)
      case _                                                     => None
    }
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
