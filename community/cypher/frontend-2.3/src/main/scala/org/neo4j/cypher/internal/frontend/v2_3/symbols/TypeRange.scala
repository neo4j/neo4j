/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.symbols

object TypeRange {
  def apply(lower: CypherType, upper: CypherType): TypeRange = TypeRange(lower, Some(upper))
}

/**
 * A TypeRange represents a path through the type tree, or an entire branch.
 * E.g. (CTAny)<-[*]-(CTInteger), or (CTNumber)<-[*]-()
 *
 * @param lower The root of the path or the branch
 * @param upper Some(type), if the TypeRange is a path through the type tree, or None, if the TypeRange is an entire branch
 */
case class TypeRange(lower: CypherType, upper: Option[CypherType]) {
  assert(upper.isEmpty || (lower isAssignableFrom upper.get), "Incompatible TypeRange bounds")

  def contains(aType: CypherType): Boolean = (lower isAssignableFrom aType) && upper.fold(true)(aType isAssignableFrom)

  def contains(that: TypeRange): Boolean = (lower isAssignableFrom that.lower) && upper.fold(true)(t => that.upper.fold(false)(_ isAssignableFrom t))

  lazy val hasDefiniteSize: Boolean = upper.isDefined || !checkForAny(lower)
  private def checkForAny: CypherType => Boolean = {
    case _: AnyType => true
    case c: CollectionType => checkForAny(c.innerType)
    case _ => false
  }

  def &(that: TypeRange): Option[TypeRange] = this intersect that
  def intersect(that: TypeRange): Option[TypeRange] = (lower greatestLowerBound that.lower).flatMap {
    newLower =>
      val newUpper = upper.fold(that.upper)(t => Some(that.upper.fold(t)(_ leastUpperBound t)))
      if (newUpper.isDefined && !(newLower isAssignableFrom newUpper.get))
        None
      else
        Some(TypeRange(newLower, newUpper))
  }

  def constrain(aType: CypherType): Option[TypeRange] = this & TypeRange(aType, None)

  /**
   * @param other the other range to determine LUBs in combination with
   * @return a set of TypeRanges that cover the LUBs for all combinations of individual types between both TypeRanges
   */
  def leastUpperBounds(other: TypeRange): Seq[TypeRange] = {
    val newLower = lower leastUpperBound other.lower
    (upper, other.upper) match {
      case (Some(u1), Some(u2)) =>
        Vector(TypeRange(newLower, Some(u1 leastUpperBound u2)))
      case (Some(u1), None)     =>
        if ((u1 isAssignableFrom other.lower) || (other.lower isAssignableFrom u1))
          Vector(TypeRange(newLower, Some(u1)))
        else
          Vector(TypeRange(newLower, Some(newLower)))
      case (None, Some(u2))     =>
        if ((u2 isAssignableFrom lower) || (lower isAssignableFrom u2))
          Vector(TypeRange(newLower, Some(u2)))
        else
          Vector(TypeRange(newLower, Some(newLower)))
      case (None, None)         =>
        if (lower == other.lower)
          Vector(TypeRange(newLower, None))
        else if (lower isAssignableFrom other.lower)
          Vector(TypeRange(newLower, Some(other.lower)), TypeRange(other.lower, None))
        else if (other.lower isAssignableFrom lower)
          Vector(TypeRange(newLower, Some(lower)), TypeRange(lower, None))
        else
          Vector(TypeRange(newLower, Some(newLower)))
    }
  }

  def reparent(f: CypherType => CypherType): TypeRange = TypeRange(f(lower), upper.map(f))
}
