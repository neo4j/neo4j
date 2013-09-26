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
package org.neo4j.cypher.internal

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap

class LRUCache[K, V](cacheSize: Int) {

  val inner = new ConcurrentLinkedHashMap.Builder[K, V]
    .maximumWeightedCapacity(cacheSize)
    .build()

  def getOrElseUpdate(key: K, f: => V): V = {
    val value = inner.get(key)

    if(value==null) {
      val createdValue = f
      val oldValue = Option(inner.putIfAbsent(key, createdValue))
      oldValue.getOrElse(createdValue)
    } else {
      value
    }
  }

  def get(key: K): Option[V] = Option(inner.get(key))

  def put(key: K, value: V) = inner.put(key, value)

  def containsKey(key: K) = inner.containsKey(key)
}
