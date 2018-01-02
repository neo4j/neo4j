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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.IdentityMap

object IdentitySet {
  private val PRESENT = new Object
  def empty[T] = IdentitySet(IdentityMap.empty[T, AnyRef])
}

case class IdentitySet[T] private (idSet: IdentityMap[T, AnyRef] = IdentityMap.empty)
  extends Set[T] with ((T) => Boolean) {
  self =>

  override def apply(elem: T): Boolean = contains(elem)

  override def contains(elem: T) = idSet.contains(elem)

  override def +(elem: T) = IdentitySet(idSet + ((elem, IdentitySet.PRESENT)))

  override def -(elem: T) = IdentitySet(idSet - elem)

  override def iterator = idSet.iterator.map(_._1)

  override def stringPrefix: String = "IdentitySet"
}
