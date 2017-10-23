/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.symbols

import scala.language.postfixOps

object TypeSpec {
  def exact(types: CypherType*): TypeSpec = exact(types)
  def exact[T <: CypherType](traversable: TraversableOnce[T]): TypeSpec = TypeSpec(traversable.map(t => TypeRange(t, t)))
  val all: TypeSpec = TypeSpec(TypeRange(CTAny, None))
  val none: TypeSpec = new TypeSpec(Vector.empty)
  def union(typeSpecs: TypeSpec*): TypeSpec = TypeSpec(typeSpecs.flatMap(_.ranges))

  def formatArguments(types: Seq[TypeSpec]) =
    s"(${types.map(_.toShortString).mkString(", ")})"

  private val simpleTypes = Vector(
    CTAny,
    CTBoolean,
    CTFloat,
    CTInteger,
    CTMap,
    CTNode,
    CTNumber,
    CTPath,
    CTRelationship,
    CTPoint,
    CTGeometry,
    CTString,
    CTGraphRef
  )

  private def apply(range: TypeRange): TypeSpec = new TypeSpec(Vector(range))
  private def apply(ranges: TraversableOnce[TypeRange]): TypeSpec = new TypeSpec(minimalRanges(ranges))

  /**
   * @param ranges a set of TypeRanges
   * @return a minimal set of TypeRanges that have the same intersection of types
   */
  private def minimalRanges(ranges: TraversableOnce[TypeRange]): Vector[TypeRange] =
    ranges.foldLeft(Vector.empty[TypeRange]) {
      case (set, range) =>
        if (set.exists(_ contains range))
          set
        else
          set.filterNot(range contains) :+ range
    }
}

/**
 * A specification of types, that can match any set of types.
 *
 * @param ranges A set of TypeRanges, the intersection of which constitutes the entire set of types matched by this specification
 */
class TypeSpec private (private val ranges: Seq[TypeRange]) extends Equals {
  def contains(that: CypherType): Boolean = contains(that, ranges)
  private def contains(that: CypherType, rs: Seq[TypeRange]): Boolean = rs.exists(_ contains that)

  def containsAny(types: CypherType*): Boolean = containsAny(TypeSpec.exact(types))
  def containsAny(that: TypeSpec): Boolean = ranges.exists {
    r1 => that.ranges.exists(r2 => (r1 constrain r2.lower).isDefined)
  }

  def union(that: TypeSpec): TypeSpec = TypeSpec(ranges ++ that.ranges)
  def |(that: TypeSpec): TypeSpec = union(that)

  def intersect(that: TypeSpec): TypeSpec = TypeSpec(ranges.flatMap {
    r => that.ranges.flatMap(r intersect)
  })
  def &(that: TypeSpec): TypeSpec = intersect(that)

  def intersectOrCoerce(that: TypeSpec): TypeSpec = {
    val intersection = intersect(that)
    if (intersection.nonEmpty)
      intersection
    else
      coercions intersect that
  }

  def coerceOrLeastUpperBound(that: TypeSpec): TypeSpec = {
    val coerced = coercions intersect that
    if (coerced.nonEmpty)
      coerced
    else
      this leastUpperBounds that
  }

  def without(aType: CypherType): TypeSpec = TypeSpec(ranges.flatMap(_ without aType))

  def constrain(that: CypherType): TypeSpec = TypeSpec(ranges.flatMap(_ constrain that))

  def constrainOrCoerce(that: CypherType): TypeSpec = {
    val constrained = constrain(that)
    if (constrained.nonEmpty)
      constrained
    else
      coercions constrain that
  }

  def leastUpperBounds(that: TypeSpec): TypeSpec = TypeSpec(ranges.flatMap {
    r => that.ranges.flatMap(r leastUpperBounds)
  })

  def wrapInList: TypeSpec = TypeSpec(ranges.map(_.reparent(CTList)))

  def wrapInCovariantList: TypeSpec = TypeSpec(ranges.map { r =>
    r.covariant.reparent(CTList)
  })

  def covariant: TypeSpec = TypeSpec(ranges.map(_.covariant))

  def unwrapLists: TypeSpec = TypeSpec(ranges.map(_.reparent { case c: ListType => c.innerType }))

  def coercions: TypeSpec = {
    val simpleCoercions = TypeSpec.simpleTypes.filter(this contains).flatMap(_.coercibleTo)
    if (this containsAny CTList(CTAny).covariant)
      TypeSpec.exact(simpleCoercions ++ CTList(CTAny).coercibleTo)
    else
      TypeSpec.exact(simpleCoercions)
  }

  def isEmpty: Boolean = ranges.isEmpty
  def nonEmpty: Boolean = !isEmpty

  lazy val hasDefiniteSize: Boolean = ranges.forall(_.hasDefiniteSize)

  def toStream: Stream[CypherType] = toStream(ranges)
  private def toStream(rs: => Seq[TypeRange]): Stream[CypherType] =
    if (rs.isEmpty)
      Stream()
    else
      TypeSpec.simpleTypes.filter(contains(_, rs)).toStream append toStream(innerTypeRanges(rs)).map(t => ListType(t))

  def iterator: Iterator[CypherType] = toStream.iterator

  override def hashCode = 41 * ranges.hashCode
  override def equals(that: Any): Boolean = that match {
    case that: TypeSpec =>
      (that canEqual this) && {
        val (finite1, infinite1) = ranges.partition(_.hasDefiniteSize)
        val (finite2, infinite2) = that.ranges.partition(_.hasDefiniteSize)
        (infinite1 == infinite2) &&
        ((finite1 == finite2) || (toStream(finite1) == toStream(finite2)))
      }
    case _              => false
  }
  override def canEqual(that: Any): Boolean = that.isInstanceOf[TypeSpec]

  def toStrings: IndexedSeq[String] = toStrings(Vector.empty, ranges, identity)
  private def toStrings(acc: IndexedSeq[String], rs: Seq[TypeRange], format: String => String): IndexedSeq[String] =
    if (rs.isEmpty)
      acc
    else if (rs.exists({ case TypeRange(_: AnyType, None) => true case _ => false }))
      acc :+ format("T")
    else
      toStrings(acc ++ TypeSpec.simpleTypes.filter(contains(_, rs)).map(t => format(t.toString)), innerTypeRanges(rs), t => s"List<${format(t)}>")

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

  override def toString = mkString("TypeSpec(", ", ", ")")

  def toShortString = mkString("", " | ", "")

  private def innerTypeRanges(rs: Seq[TypeRange]): Seq[TypeRange] = rs.flatMap {
    case TypeRange(c: ListType, Some(u: ListType)) => Some(TypeRange(c.innerType, u.innerType))
    case TypeRange(c: ListType, None)                    => Some(TypeRange(c.innerType, None))
    case TypeRange(_: AnyType, Some(u: ListType))        => Some(TypeRange(CTAny, u.innerType))
    case r@TypeRange(_: AnyType, None)                         => Some(r)
    case _                                                     => None
  }
}
