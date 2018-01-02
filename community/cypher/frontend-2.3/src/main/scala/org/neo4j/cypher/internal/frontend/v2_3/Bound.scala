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
package org.neo4j.cypher.internal.frontend.v2_3

sealed trait Bound[+V] {
  def endPoint: V
  def inequalitySignSuffix: String

  def map[P](f: V => P): Bound[P]

  def isInclusive: Boolean
}

final case class InclusiveBound[+V](endPoint: V) extends Bound[V] {
  val inequalitySignSuffix = "="

  override def map[P](f: V => P): InclusiveBound[P] = copy(endPoint = f(endPoint))

  def isInclusive: Boolean = true
}

final case class ExclusiveBound[+V](endPoint: V) extends Bound[V] {
  val inequalitySignSuffix = ""

  override def map[P](f: V => P): ExclusiveBound[P] = copy(endPoint = f(endPoint))

  def isInclusive: Boolean = false
}
