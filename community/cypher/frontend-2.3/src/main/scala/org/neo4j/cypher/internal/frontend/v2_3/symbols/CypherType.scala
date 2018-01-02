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

abstract class CypherType {
  def parentType: CypherType
  val isAbstract: Boolean = false

  def coercibleTo: Set[CypherType] = Set.empty

  def parents: Seq[CypherType] = parents(Vector.empty)
  private def parents(accumulator: Seq[CypherType]): Seq[CypherType] =
    if (this.parentType == this)
      accumulator
    else
      this.parentType.parents(accumulator :+ this.parentType)

  /*
  Determines if the class or interface represented by this
  {@code CypherType} object is either the same as, or is a
  supertype of, the class or interface represented by the
  specified {@code CypherType} parameter.
   */
  def isAssignableFrom(other: CypherType): Boolean =
    if (other == this)
      true
    else if (other.parentType == other)
      false
    else
      isAssignableFrom(other.parentType)

  def legacyIteratedType: CypherType = this

  def leastUpperBound(other: CypherType): CypherType =
    if (this.isAssignableFrom(other)) this
    else if (other.isAssignableFrom(this)) other
    else parentType leastUpperBound other.parentType

  def greatestLowerBound(other: CypherType): Option[CypherType] =
    if (this.isAssignableFrom(other)) Some(other)
    else if (other.isAssignableFrom(this)) Some(this)
    else None

  lazy val covariant: TypeSpec = TypeSpec.all constrain this
  lazy val invariant: TypeSpec = TypeSpec.exact(this)
  lazy val contravariant: TypeSpec = TypeSpec.all leastUpperBounds this

  def rewrite(f: CypherType => CypherType) = f(this)
}
