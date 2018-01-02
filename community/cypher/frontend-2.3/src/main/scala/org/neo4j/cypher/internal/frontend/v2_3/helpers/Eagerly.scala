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
package org.neo4j.cypher.internal.frontend.v2_3.helpers

import scala.collection.{immutable, mutable}

object Eagerly {

  def immutableReplaceKeys[K, V](m: immutable.Map[K, V])(replacements: (K, K)*): immutable.Map[K, V] = {
    val deletes = replacements.map { case (oldKey, _) => oldKey }
    val updates = replacements.flatMap {
      case (oldKey, newKey) => m.get(oldKey).map(value => newKey -> value)
    }
    m -- deletes ++ updates
  }

  // These two methods could in theory be replaced by a single one. My attempts so far didn't type out or broke scalac. You get a cookie if you get it to work -- boggle

  def immutableMapValues[A, B, C](m: collection.Map[A, B], f: B => C): immutable.Map[A, C] =
    mapToBuilder(m, f, immutable.Map.newBuilder[A, C])

  def mutableMapValues[A, B, C](m: collection.Map[A, B], f: B => C): mutable.Map[A, C] =
    mapToBuilder(m, f, mutable.Map.newBuilder[A, C])

  private def mapToBuilder[A, B, C, To](m: collection.Map[A, B], f: B => C, builder: mutable.Builder[(A,C ), To]): To = {
    builder.sizeHint(m.size)
    m.foldLeft(builder) { case (acc, (k, v)) => acc += ((k, f(v))) }
    builder.result()
  }
}
