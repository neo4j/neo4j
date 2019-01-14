/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.util.v3_4

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
