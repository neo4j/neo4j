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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import scala.reflect._

/**
 * Map implementation optimized for fast writes via putValues.
 *
 * Stores values in an array and has a lookup table from key to array index for doing lookups.
 * @param keyToIndexMap the mapping from keys to array indexes
 */
class ArrayBackedMap[K, V](keyToIndexMap: Map[K, Int])(implicit val tag: ClassTag[V]) extends Map[K, V] {
  private var valueArray: Array[V] = null

  /**
   * Writes values by reference straight into the map.
   * When using this make sure the order matches the order specified by keyToIndexMap.
   */
  def putValues(array: Array[V]) = {
    valueArray = array
  }

  /**
   * Creates a copy of the map
   * @return a copy of the map
   */
  def copy = {
    val newArray = new Array[V](valueArray.length)
    System.arraycopy(valueArray, 0, newArray, 0, valueArray.length)
    val newMap = new ArrayBackedMap[K, V](keyToIndexMap)
    newMap.putValues(newArray)
    newMap
  }

  override def get(key: K): Option[V] = keyToIndexMap.get(key).map{ index =>
    if (valueArray != null && index < valueArray.length)
      valueArray(index)
    else
      null.asInstanceOf[V]
  }

  override def iterator: Iterator[(K, V)] = new Iterator[(K, V)]() {
    private val inner = keyToIndexMap.iterator

    override def hasNext: Boolean = inner.hasNext

    override def next(): (K, V) = {
      val (key, index) = inner.next()
      if (valueArray != null && index < valueArray.length) (key, valueArray(index))
      else (key, null.asInstanceOf[V])
    }
  }

  /**
   * @note This class is not optimized for this operation, so this implementation is not particularly efficient.
   *       (Avoid using it if possible)
   */
  override def +[B1 >: V](kv: (K, B1)): Map[K, B1] = {
    val (key, value) = kv
    val index = keyToIndexMap.get(key)
    index match {
      //key already existed in map, copy over values and create new map
      case Some(i) =>
        val newArray = new Array[V](valueArray.length)
        System.arraycopy(valueArray, 0, newArray, 0, valueArray.length)
        newArray(i) = value.asInstanceOf[V]
        val newMap: ArrayBackedMap[K, V] = new ArrayBackedMap[K, V](keyToIndexMap)
        newMap.putValues(newArray)
        newMap
      //key was not in map, create new map and add new key-value pair at the end of the its valueArray
      case None => {
        val newHeadersMap = keyToIndexMap.updated(key, valueArray.length)
        val newMap = new ArrayBackedMap[K, V](newHeadersMap)
        val newArray = new Array[V](valueArray.length + 1)
        System.arraycopy(valueArray, 0, newArray, 0, valueArray.length)
        newArray(valueArray.length) = value.asInstanceOf[V]
        newMap.putValues(newArray)
        newMap
      }
    }
  }

  /**
   * @note This class is not optimized for this operation, so this implementation is not particularly efficient.
   *       (Avoid using it if possible)
   */
  override def -(key: K): Map[K, V] = {
    val index = keyToIndexMap.get(key)
    index match {
      case Some(indexToRemove) => {
        // Create a new headerToIndex map without the key to be removed
        // Note: We need to update the indexes of the new headers map to match the smaller array
        val newHeadersMap = (keyToIndexMap - key).mapValues(i => if (i >= indexToRemove) i - 1 else i)

        // Create a new array by first filtering out the index to be removed
        val newArray = valueArray.indices.filterNot(_ == indexToRemove).map(valueArray).toArray

        val newMap = new ArrayBackedMap[K, V](newHeadersMap)
        newMap.putValues(newArray)
        newMap
      }
      case None => this
    }
  }
}

object ArrayBackedMap {
  def apply[K, V](keys: K*)(implicit tag: ClassTag[V]): ArrayBackedMap[K, V] = new ArrayBackedMap[K, V](keys.zipWithIndex.toMap)
}
