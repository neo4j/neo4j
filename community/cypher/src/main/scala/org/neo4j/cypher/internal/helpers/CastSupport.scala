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
package org.neo4j.cypher.internal.helpers

import org.neo4j.cypher.CypherTypeException

object CastSupport {

  // TODO Switch to using ClassTag once we decide to depend on the reflection api

  /**
   * Filter sequence by type
   *
   * @param seq Input elements to filter
   * @tparam A Required super type of type erasure of remaining elements
   * @return seq without any element whose type erasure does not conform to A
   */
  def sift[A : Manifest](seq: Seq[Any]): Seq[A] = seq.collect(erasureCast)

  /**
   * Casts input to A if possible according to type erasure, discards input otherwise
   *
   * @tparam A the required super type to which arguments should be cast
   * @return PartialFunction that is the identity function on all instances of the type erasure of A
   */
  def erasureCast[A : Manifest]: PartialFunction[Any, A] = { case value: A => value }

  def erasureCastOrFail[A](value: Any)(implicit ev: Manifest[A]): A = value match {
    case v: A => v
    case _    => throw new CypherTypeException(
      s"Expected ${value} to be a ${ev.runtimeClass.getName}, but it was a ${value.getClass.getName}")
  }
}
