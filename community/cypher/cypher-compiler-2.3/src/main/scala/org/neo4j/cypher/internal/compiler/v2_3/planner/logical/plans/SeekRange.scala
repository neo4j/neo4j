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

sealed trait SeekRange[V]
sealed trait HalfOpenSeekRange[V] extends SeekRange[V]

final case class RangeBetween[V](lower: Bound[V], upper: Bound[V]) extends SeekRange[V]
final case class RangeGreaterThan[V](lower: Bound[V]) extends HalfOpenSeekRange[V]
final case class RangeLessThan[V](lower: Bound[V]) extends HalfOpenSeekRange[V]

final case class PrefixRange(prefix: String) extends SeekRange[String]

sealed trait Bound[V] {
  def endPoint: V
}

final case class InclusiveBound[V](endPoint: V) extends Bound[V]
final case class ExclusiveBound[V](endPoint: V) extends Bound[V]




