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
  def apply(types: CypherType*): TypeSet = new DiscreteTypeSet(types.toSet)
  def apply[T <: CypherType](traversable: TraversableOnce[T]): TypeSet = new DiscreteTypeSet(traversable.toSet)

  val empty: TypeSet = new DiscreteTypeSet(Set.empty[CypherType])
  val all: TypeSet = RangedTypeSet(Set(Range(AnyType(), None)))

  class DiscreteTypeSet(set: Set[CypherType]) extends TypeSet {
    def contains(elem: CypherType): Boolean = set.contains(elem)
    def +(elem: CypherType): TypeSet = new DiscreteTypeSet(set.+(elem))
    def -(elem: CypherType): TypeSet = new DiscreteTypeSet(set.-(elem))
    def iterator: Iterator[CypherType] = set.iterator
    override def equals(that: Any) = that match {
      case _: RangedTypeSet => that.equals(this)
      case _ => set.equals(that)
    }
    override def hashCode(): Int = set.hashCode()

    def constrain(other: TypeSet): TypeSet = {
      TypeSet(filter {
        t => other.exists(_.isAssignableFrom(t))
      })
    }

    def mergeDown(other: TypeSet): TypeSet = other match {
      case _: RangedTypeSet =>
        RangedTypeSet(set.map(Range(_))).mergeDown(other)
      case _                =>
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


  object Range {
    def apply(aType: CypherType): Range = Range(aType, Some(aType))
  }

  case class Range(lower: CypherType, upper: Option[CypherType]) {
    assert(upper.isEmpty || (lower isAssignableFrom upper.get))

    def contains(aType: CypherType): Boolean = (lower isAssignableFrom aType) && upper.fold(true)(aType isAssignableFrom _)

    def hasDefiniteSize: Boolean = upper.isDefined || !checkForAny(lower)
    private def checkForAny: CypherType => Boolean = {
      case _: AnyType => true
      case c: CollectionType => checkForAny(c.innerType)
      case _ => false
    }

    def constrain(other: Range): Option[Range] = (lower mergeUp other.lower).flatMap {
      newLower =>
        val newUpper = upper.fold(other.upper)(t => Some(other.upper.fold(t)(_ mergeDown t)))
        if (newUpper.isDefined && !(newLower isAssignableFrom newUpper.get))
          None
        else
          Some(Range(newLower, newUpper))
    }

    def mergeDown(other: Range): Seq[Range] = {
      val newLower = lower mergeDown other.lower
      (upper, other.upper) match {
        case (Some(u1), Some(u2)) =>
          Seq(Range(newLower, Some(u1 mergeDown u2)))
        case (Some(u1), None)     =>
          if ((u1 isAssignableFrom other.lower) || (other.lower isAssignableFrom u1))
            Seq(Range(newLower, Some(u1)))
          else
            Seq(Range(newLower, Some(newLower)))
        case (None, Some(u2))     =>
          if ((u2 isAssignableFrom lower) || (lower isAssignableFrom u2))
            Seq(Range(newLower, Some(u2)))
          else
            Seq(Range(newLower, Some(newLower)))
        case (None, None)         =>
          if (lower == other.lower)
            Seq(Range(newLower, None))
          else if (lower isAssignableFrom other.lower)
            Seq(Range(newLower, Some(other.lower)), Range(other.lower, None))
          else if (other.lower isAssignableFrom lower)
            Seq(Range(newLower, Some(lower)), Range(lower, None))
          else
            Seq(Range(newLower, Some(newLower)))
      }
    }

    def reparent(f: CypherType => CypherType): Range = Range(f(lower), upper.map(f))
  }


  case class RangedTypeSet(ranges: Set[Range]) extends TypeSet {
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

    def contains(aType: CypherType): Boolean = contains(aType, ranges)
    private def contains(aType: CypherType, rs: Set[Range]): Boolean = rs.exists(_ contains aType)

    def constrain(other: TypeSet): RangedTypeSet = other match {
      case dts: DiscreteTypeSet => constrain(copy(dts.map(Range(_, None))))
      case rts: RangedTypeSet   => copy(ranges.flatMap {
        r1 => rts.ranges.flatMap(r1 constrain _)
      })
    }

    def mergeDown(other: TypeSet): RangedTypeSet = other match {
      case dts: DiscreteTypeSet => mergeDown(copy(dts.map(Range(_))))
      case rts: RangedTypeSet   => copy(ranges.flatMap {
        r1 => rts.ranges.flatMap(r1 mergeDown _)
      })
    }

    def mergeUp(other: TypeSet): TypeSet = ???

    def +(elem: CypherType): Set[CypherType] = copy(ranges + Range(elem, Some(elem)))
    def -(elem: CypherType): Set[CypherType] = ???

    override def reparent(f: CypherType => CypherType): RangedTypeSet = copy(ranges.map(_.reparent(f)))

    override def isEmpty: Boolean = ranges.isEmpty
    override def nonEmpty: Boolean = !isEmpty

    override def hasDefiniteSize: Boolean = _hasDefiniteSize
    private lazy val _hasDefiniteSize = ranges.forall(_.hasDefiniteSize)

    override def toStream: Stream[CypherType] = toStream(ranges)
    private def toStream(rs: Set[Range]): Stream[CypherType] =
      if (rs.isEmpty) Stream()
      else simpleTypes.filter(contains(_, rs)).toStream #::: toStream(innerTypeRanges(rs)).map(t => CollectionType(t): CypherType)

    def iterator: Iterator[CypherType] = toStream.iterator

    override def foreach[U](f: CypherType => U): Unit = {
      if (!hasDefiniteSize)
        throw new UnsupportedOperationException("Cannot map over indefinite collection")
      super.foreach(f)
    }

    override def equals(that: Any): Boolean = that match {
      case that: RangedTypeSet =>
        (that canEqual this) && {
          val (finite1, infinite1) = ranges.partition(_.hasDefiniteSize)
          val (finite2, infinite2) = that.ranges.partition(_.hasDefiniteSize)
          (infinite1 == infinite2) &&
            ((finite1 == finite2) || (RangedTypeSet(finite1).toStream == RangedTypeSet(finite2).toStream))
        }
      case _                   =>
        if (!hasDefiniteSize) false
        else super.equals(that)
    }

    override def stringPrefix: String = "RangedTypeSet"
    override def toString: String = typeStrings(ranges).mkString(stringPrefix + "(", ", ", ")")

    def formattedString: String = {
      val types = typeStrings(ranges)
      types.length match {
        case 0 => ""
        case 1 => types.head
        case _ => s"${types.dropRight(1).mkString(", ")} or ${types.last}"
      }
    }

    private def typeStrings: Set[Range] => IndexedSeq[String] = rs => {
      if (rs.isEmpty) Vector()
      else if (rs.exists({ case Range(_: AnyType, None) => true case _ => false })) Vector("<T>")
      else simpleTypes.filter(contains(_, rs)).map(_.toString).toIndexedSeq ++ typeStrings(innerTypeRanges(rs)).map(t => s"Collection<$t>")
    }

    private def innerTypeRanges: Set[Range] => Set[Range] = _.flatMap {
      case Range(c: CollectionType, Some(u: CollectionType)) => Some(Range(c.innerType, Some(u.innerType)))
      case Range(c: CollectionType, None)                    => Some(Range(c.innerType, None))
      case Range(_: AnyType, None)                           => Some(Range(AnyType(), None))
      case _                                                 => None
    }
  }
}


sealed abstract class TypeSet extends Set[CypherType] {
  def constrain(types: CypherType*): TypeSet = constrain(TypeSet(types:_*))
  def constrain(other: TypeSet): TypeSet
  def mergeDown(types: CypherType*): TypeSet = mergeDown(TypeSet(types:_*))
  def mergeDown(other: TypeSet): TypeSet
  def mergeUp(other: TypeSet): TypeSet

  def reparent(f: CypherType => CypherType): TypeSet = map(f)

  def formattedString: String
}
