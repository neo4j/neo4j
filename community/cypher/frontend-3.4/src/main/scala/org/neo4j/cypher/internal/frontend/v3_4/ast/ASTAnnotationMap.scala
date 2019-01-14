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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.{ASTNode, Eagerly, InputPosition}

object ASTAnnotationMap {
  def empty[K <: ASTNode, V]: ASTAnnotationMap[K, V] = new ASTAnnotationMap(Map.empty[(K, InputPosition), V])
  def apply[K <: ASTNode, V](elems: (K, V)*): ASTAnnotationMap[K, V] =
    new ASTAnnotationMap[K, V](Map(elems.map { case (astnode, value) => ((astnode, astnode.position), value)}: _*))
}

class ASTAnnotationMap[K <: ASTNode, V] private (store: Map[(K, InputPosition), V]) extends Map[K, V] {

  def keyPositionSet: Set[(K, InputPosition)] = store.keySet

  override def +[B1 >: V](kv: (K, B1)): ASTAnnotationMap[K, B1] =
    new ASTAnnotationMap(store + (((kv._1, kv._1.position), kv._2)))

  override def get(key: K): Option[V] =
    store.get((key, key.position))

  override def updated[V1 >: V](key: K, value: V1): ASTAnnotationMap[K, V1] =
    this + ((key, value))

  override def iterator: Iterator[(K, V)] =
    store.iterator.map { case ((astNode, _), value) => (astNode, value) }

  override def -(key: K): Map[K, V] =
    new ASTAnnotationMap(store - ((key, key.position)))

  override def mapValues[C](f: (V) => C): ASTAnnotationMap[K, C] =
    new ASTAnnotationMap(Eagerly.immutableMapValues(store, f))

  def replaceKeys(replacements: (K, K)*) = {
    val expandedReplacements = replacements.map {
      case (oldKey, newKey) => (oldKey -> oldKey.position) -> (newKey -> newKey.position)
    }
    val newStore = Eagerly.immutableReplaceKeys(store)(expandedReplacements: _*)
    new ASTAnnotationMap(newStore)
  }
}
