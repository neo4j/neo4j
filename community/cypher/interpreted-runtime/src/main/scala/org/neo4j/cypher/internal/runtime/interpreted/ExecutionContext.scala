/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual._

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{Iterator, immutable}

object ExecutionContext {
  def empty: ExecutionContext = apply()

  def from(x: (String, AnyValue)*): ExecutionContext = apply().set(x)

  def apply(m: MutableMap[String, AnyValue] = MutableMaps.empty) = MapExecutionContext(m)
}

trait ExecutionContext extends MutableMap[String, AnyValue] {
  def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit
  def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit
  def setLongAt(offset: Int, value: Long): Unit
  def getLongAt(offset: Int): Long
  def longs(): Array[Long]

  def setRefAt(offset: Int, value: AnyValue): Unit
  def getRefAt(offset: Int): AnyValue
  def refs(): Array[AnyValue]

  def set(newEntries: Seq[(String, AnyValue)]): ExecutionContext
  def set(key: String, value: AnyValue): ExecutionContext
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext
  def mergeWith(other: ExecutionContext): ExecutionContext
  def createClone(): ExecutionContext

  def copyWith(key: String, value: AnyValue): ExecutionContext
  def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext
  def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext
  def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext

  // Needed by legacy pattern matcher. Returns a map of all bound nodes/relationships in the context.
  // Entities that are only references (ids) are materialized with the provided materialization functions
  def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue]

  def isNull(key: String): Boolean
}

case class MapExecutionContext(m: MutableMap[String, AnyValue])
  extends ExecutionContext {

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit = fail()

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()
  override def getLongAt(offset: Int): Long = fail()
  override def longs(): Array[Long] = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()
  override def getRefAt(offset: Int): AnyValue = fail()
  override def refs(): Array[AnyValue] = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a map context as a primitive context")

  override def get(key: String): Option[AnyValue] = m.get(key)

  override def iterator: Iterator[(String, AnyValue)] = m.iterator

  override def size: Int = m.size

  override def mergeWith(other: ExecutionContext): ExecutionContext = other match {
    case MapExecutionContext(otherMap) => copy(m = m ++ otherMap)
    case _ => fail()
  }

  override def foreach[U](f: ((String, AnyValue)) => U) {
    m.foreach(f)
  }

  override def +=(kv: (String, AnyValue)): MapExecutionContext.this.type = {
    m += kv
    this
  }

  override def toMap[T, U](implicit ev: (String, AnyValue) <:< (T, U)): immutable.Map[T, U] = m.toMap(ev)

  override def set(newEntries: Seq[(String, AnyValue)]): MapExecutionContext.this.type =
    createWithNewMap(m.clone() ++= newEntries)

  // This may seem silly but it has measurable impact in tight loops

  override def set(key: String, value: AnyValue): MapExecutionContext.this.type = {
    val newMap = m.clone()
    newMap.put(key, value)
    createWithNewMap(newMap)
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): MapExecutionContext.this.type = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    createWithNewMap(newMap)
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): MapExecutionContext.this.type = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    newMap.put(key3, value3)
    createWithNewMap(newMap)
  }

  override def copyWith(key: String, value: AnyValue): MapExecutionContext.this.type =
    set(key, value)

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): MapExecutionContext.this.type = {
    set(key1, value1, key2, value2)
  }

  override def copyWith(key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext =
    set(key1, value1, key2, value2, key3, value3)

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = set(newEntries)

  override def createClone(): ExecutionContext = createWithNewMap(m.clone())

  protected def createWithNewMap(newMap: MutableMap[String, AnyValue]): this.type = {
    copy(m = newMap).asInstanceOf[this.type]
  }

  override def -=(key: String): this.type = {
    m.remove(key)
    this
  }

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] =
    m.collect {
      case kv @ (_, _: NodeValue) =>
        kv
      case kv @ (_, _: RelationshipValue) =>
        kv
      case (k, v: NodeReference) =>
        (k, materializeNode(v.id()))
      case (k, v: RelationshipReference) =>
        (k, materializeRelationship(v.id()))
    }.toMap

  override def isNull(key: String): Boolean =
    get(key) match {
      case Some(Values.NO_VALUE) => true
      case _ => false
    }
}
