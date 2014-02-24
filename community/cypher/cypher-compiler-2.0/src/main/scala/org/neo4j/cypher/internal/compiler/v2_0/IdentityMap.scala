/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import scala.collection.JavaConverters._
import java.util

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
  def get(key: K): Option[V] = {
    val value = idMap.get(key)
    if (value == null)
      None
    else
      Some(value)
  }

  def +[V1 >: V](kv: (K, V1)): IdentityMap[K, V1] =
    IdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V1]]
      clone.put(kv._1, kv._2)
      clone
    })

  def -(key: K): IdentityMap[K, V] =
    IdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V]]
      clone.remove(key)
      clone
    })

  override def updated[V1 >: V](key: K, value: V1): IdentityMap[K, V1] =
    IdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V1]]
      clone.put(key, value)
      clone
    })

  def iterator: Iterator[(K, V)] =
    idMap.clone().asInstanceOf[util.IdentityHashMap[K, V]].entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))

  override def stringPrefix: String = "IdentityMap"
}


object MutableIdentityMap {
  def empty[K, V]: MutableIdentityMap[K, V] = MutableIdentityMap()
  def apply[K, V](elems: (K, V)*): MutableIdentityMap[K, V] = {
    val idMap = new util.IdentityHashMap[K, V]()
    elems.foreach {
      elem => idMap.put(elem._1, elem._2)
    }
    MutableIdentityMap(idMap)
  }
}

case class MutableIdentityMap[K, V] private (idMap: util.IdentityHashMap[K, V] = new util.IdentityHashMap[K, V]()) extends Map[K, V] {
  def get(key: K): Option[V] = {
    val value = idMap.get(key)
    if (value == null)
      None
    else
      Some(value)
  }

  def put(key: K, value: V): Option[V] = {
    val r = get(key)
    update(key, value)
    r
  }

  def update(key: K, value: V) {
    idMap.put(key, value)
  }

  def remove(key: K): Option[V] = {
    val r = get(key)
    this -= key
    r
  }

  override def +[V1 >: V](kv: (K, V1)): MutableIdentityMap[K, V1] =
    MutableIdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V1]]
      clone.put(kv._1, kv._2)
      clone
    })

  def +=(kv: (K, V)): MutableIdentityMap[K, V] = {
    update(kv._1, kv._2)
    this
  }

  override def -(key: K): MutableIdentityMap[K, V] =
    MutableIdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V]]
      clone.remove(key)
      clone
    })

  def -=(key: K): MutableIdentityMap[K, V] = {
    idMap.remove(key)
    this
  }

  override def updated[V1 >: V](key: K, value: V1): MutableIdentityMap[K, V1] =
    MutableIdentityMap({
      val clone = idMap.clone().asInstanceOf[util.IdentityHashMap[K, V1]]
      clone.put(key, value)
      clone
    })

  override def iterator: Iterator[(K, V)] =
    idMap.entrySet().iterator().asScala.map(e => (e.getKey, e.getValue))

  override def stringPrefix: String = "MutableIdentityMap"
}
