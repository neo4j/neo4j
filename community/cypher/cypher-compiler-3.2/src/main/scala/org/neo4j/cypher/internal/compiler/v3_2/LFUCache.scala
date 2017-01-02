/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2

import java.util.function.Function

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}

class LFUCache[K <: AnyRef, V <: AnyRef](val size: Int) extends ((K, => V) => V) {

  val inner: Cache[K, V] = Caffeine.newBuilder().maximumSize(size).build[K, V]()

  def getOrElseUpdate(key: K, f: => V): V = inner.get(key, new Function[K, V] {
    override def apply(t: K): V = f
  })

  def getOrElseUpdateByKey(key: K, f: K => V): V = inner.get(key, new Function[K, V] {
    override def apply(t: K): V = f(t)
  })

  def get(key: K): Option[V] = Option(inner.getIfPresent(key))

  def put(key: K, value: V) = inner.put(key, value)

  def +=(kv: (K, V)) = put(kv._1, kv._2)

  def remove(key: K): Option[V] = Option(inner.asMap().remove(key))

  def containsKey(key: K) = inner.asMap().containsKey(key)

  def apply(key: K, value: => V): V = getOrElseUpdate(key, value)
}
