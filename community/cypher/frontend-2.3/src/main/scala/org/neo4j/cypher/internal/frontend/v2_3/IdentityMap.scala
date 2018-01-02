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

import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.Map

object IdentityMap {
  def empty[K, V]: IdentityMap[K, V] = IdentityMap()

  def apply[K, V](elems: (K, V)*): IdentityMap[K, V] = {
    val idMap = new util.IdentityHashMap[K, V]()
    elems.foreach {
      elem => idMap.put(elem._1, elem._2)
    }
    IdentityMap(idMap)
  }
}

case class IdentityMap[K, V] private (idMap: util.IdentityHashMap[K, V] = new util.IdentityHashMap[K, V]()) extends Map[K, V] {
  self =>

  override def get(key: K): Option[V] =
    idMap.get(key) match {
      case null => None
      case value => Some(value)
    }

  override def +[V1 >: V](kv: (K, V1)): IdentityMap[K, V1] =
    IdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V1]]
      clone.put(kv._1, kv._2)
      clone
    })

  override def -(key: K): IdentityMap[K, V] =
    IdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V]]
      clone.remove(key)
      clone
    })

  override def updated[V1 >: V](key: K, value: V1): IdentityMap[K, V1] = this + ((key, value))

  override def iterator: Iterator[(K, V)] =
    idMap.clone().asInstanceOf[util.IdentityHashMap[K, V]].entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))

  override def stringPrefix: String = "IdentityMap"
}
