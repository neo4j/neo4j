/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

sealed trait SeekRange[+V]

sealed trait MappableSeekRange[+V] extends SeekRange[V] {
  def map[P](f: V => P): MappableSeekRange[P]
}

sealed trait HalfOpenSeekRange[+V] extends MappableSeekRange[V] {
  def bound: Bound[V]
  def sign: String

  override def map[P](f: V => P): HalfOpenSeekRange[P]

  // Given compareResult is the result of comparing a value against this range's bound using some ordering,
  // this method returns true if the value falls into this range
  def compares(compareResult: Int): Boolean
}

final case class RangeBetween[+V](lower: Bound[V], upper: Bound[V]) extends MappableSeekRange[V] {
  override def map[P](f: V => P): RangeBetween[P] = copy(lower = lower.map(f), upper = upper.map(f))
}

final case class RangeGreaterThan[+V](bound: Bound[V]) extends HalfOpenSeekRange[V] {
  val sign = s">${bound.sign}"

  override def map[P](f: V => P): RangeGreaterThan[P] = copy(bound = bound.map(f))

  def compares(compareResult: Int): Boolean =
    if (bound.isInclusive) compareResult >= 0 else compareResult > 0
}

final case class RangeLessThan[+V](bound: Bound[V]) extends HalfOpenSeekRange[V] {
  val sign = s"<${bound.sign}"

  override def map[P](f: V => P): RangeLessThan[P] = copy(bound = bound.map(f))

  def compares(compareResult: Int): Boolean =
    if (bound.isInclusive) compareResult <= 0 else compareResult < 0
}

final case class PrefixRange(prefix: String) extends SeekRange[String]

sealed trait Bound[+V] {
  def endPoint: V
  def sign: String

  def map[P](f: V => P): Bound[P]

  def isInclusive: Boolean
  def isExclusive: Boolean
}

final case class InclusiveBound[+V](endPoint: V) extends Bound[V] {
  val sign = "="

  override def map[P](f: V => P): InclusiveBound[P] = copy(endPoint = f(endPoint))

  def isInclusive: Boolean = true
  def isExclusive: Boolean = false
}

final case class ExclusiveBound[+V](endPoint: V) extends Bound[V] {
  val sign = ""

  override def map[P](f: V => P): ExclusiveBound[P] = copy(endPoint = f(endPoint))

  def isInclusive: Boolean = false
  def isExclusive: Boolean = true
}




