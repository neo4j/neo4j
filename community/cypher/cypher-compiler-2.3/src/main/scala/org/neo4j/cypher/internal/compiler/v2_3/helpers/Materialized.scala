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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import scala.collection.{TraversableLike, immutable, mutable}

// This is deprecated. All these helper classes should move
// to cypher compiler so that we may change API between
// versions
//
// The replacement is called org.neo4j.internal.helpers.Eagerly
//
@deprecated
object Materialized {

  def mapValues[A, B, C](m: collection.Map[A, B], f: B => C): Map[A, C] = {
    val builder: mutable.Builder[(A, C), Map[A, C]] = mapBuilder(m)

    for ( ((k, v)) <- m )
      builder += k -> f(v)
    builder.result()
  }

  def mapBuilder[A, B](underlying: TraversableLike[_, _]): mutable.Builder[(A, B), Map[A, B]] = {
    val builder = immutable.Map.newBuilder[A, B]
    builder.sizeHint(underlying)
    builder
  }
}
