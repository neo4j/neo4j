/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3

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
