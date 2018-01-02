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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.frontend.v2_3.helpers.Eagerly

object ASTAnnotationMap {
  def empty[K <: ASTNode, V]: ASTAnnotationMap[K, V] = new ASTAnnotationMap(Map.empty[(K, InputPosition), V])
  def apply[K <: ASTNode, V](elems: (K, V)*): ASTAnnotationMap[K, V] =
    new ASTAnnotationMap[K, V](Map(elems.map { case (astnode, value) => ((astnode, astnode.position), value)}: _*))
}

class ASTAnnotationMap[K <: ASTNode, V] private (store: Map[(K, InputPosition), V]) extends Map[K, V] {
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
