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
package org.neo4j.cypher.internal.compiler.v2_2.helpers

object CachedFunction {
  def byIdentity[A, B](f: A => B) = new (A => B) {
    private val cache = new java.util.IdentityHashMap[A, B]()

    def apply(input: A): B = cache.get(input) match {
      case null =>
        val newValue = f(input)
        cache.put(input, newValue)
        newValue
      case value =>
        value
    }
  }

  def byIdentity[A, B, C](f: (A, B) => C): (A, B) => C =
    Function.untupled(byIdentity(f.tupled))

  def byIdentity[A, B, C, D](f: (A, B, C) => D): (A, B, C) => D =
    Function.untupled(byIdentity(f.tupled))
}
