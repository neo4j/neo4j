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
    case c: ListType => checkForAny(c.innerType)
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

  def covariant = copy(upper = None)

  def constrain(aType: CypherType): Option[TypeRange] = this & TypeRange(aType, None)

  def without(aType: CypherType): Option[TypeRange] = {
    if (aType.isAssignableFrom(lower)) {
      None
    } else if (lower.isAssignableFrom(aType)) {
      upper match {
        case None => Some(TypeRange(lower, aType.parentType))
        case Some(up) =>
          if (aType.isAssignableFrom(up))
            Some(TypeRange(lower, aType.parentType))
          else
            Some(this)
      }
    } else {
      Some(this)
    }
  }

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
