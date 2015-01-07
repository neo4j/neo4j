/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

object TypeRange {
  def apply(lower: CypherType, upper: CypherType): TypeRange = TypeRange(lower, Some(upper))
}

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
  def intersect(that: TypeRange): Option[TypeRange] = (lower mergeDown that.lower).flatMap {
    newLower =>
      val newUpper = upper.fold(that.upper)(t => Some(that.upper.fold(t)(_ mergeUp t)))
      if (newUpper.isDefined && !(newLower isAssignableFrom newUpper.get))
        None
      else
        Some(TypeRange(newLower, newUpper))
  }

  def constrain(aType: CypherType): Option[TypeRange] = this & TypeRange(aType, None)

  def mergeUp(other: TypeRange): Seq[TypeRange] = {
    val newLower = lower mergeUp other.lower
    (upper, other.upper) match {
      case (Some(u1), Some(u2)) =>
        Vector(TypeRange(newLower, Some(u1 mergeUp u2)))
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
