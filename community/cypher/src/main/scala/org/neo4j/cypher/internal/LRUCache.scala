/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap

class LRUCache[K, V](cacheSize: Int) {

  class LazyValue(f: => V) {
    lazy val value = f
  }

  val inner = new ConcurrentLinkedHashMap.Builder[K, LazyValue]
    .maximumWeightedCapacity(cacheSize)
    .build()

  def getOrElseUpdate(key: K, f: => V): V = {
    val oldValue = inner.putIfAbsent(key, new LazyValue(f))
    if (oldValue == null) {
      f
    } else {
      oldValue.value
    }
  }

  def get(key: K): Option[V] = Option(inner.get(key).value)

  def put(key: K, value: V) = inner.put(key, new LazyValue(value))

  def containsKey(key: K) = inner.containsKey(key)
}

