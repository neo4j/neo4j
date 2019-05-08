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

import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v3_5.util.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual._

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{Iterator, immutable}

object ExecutionContext {
  def empty: ExecutionContext = apply()

  def from(x: (String, AnyValue)*): ExecutionContext = {
    val context = empty
    context.set(x)
    context
  }

  def apply(m: MutableMap[String, AnyValue] = MutableMaps.empty): MapExecutionContext = new MapExecutionContext(m, null)
}

trait ExecutionContext extends MutableMap[String, AnyValue] {
  def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit
  def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit
  def copyCachedFrom(input: ExecutionContext): Unit
  def setLongAt(offset: Int, value: Long): Unit
  def getLongAt(offset: Int): Long

  def setRefAt(offset: Int, value: AnyValue): Unit
  def getRefAt(offset: Int): AnyValue

  def set(newEntries: Seq[(String, AnyValue)]): Unit
  def set(key: String, value: AnyValue): Unit
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit
  def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit
  def mergeWith(other: ExecutionContext, entityById: EntityById): Unit
  def createClone(): ExecutionContext

  def setCachedProperty(key: CachedNodeProperty, value: Value): Unit
  def setCachedPropertyAt(offset: Int, value: Value): Unit

  /**
    * Returns the cached node property value
    *   or NO_VALUE if the node does not have the property,
    *   or null     if this cached value has been invalidated.
    */
  def getCachedProperty(key: CachedNodeProperty): Value

  /**
    * Returns the cached node property value
    *   or NO_VALUE if the node does not have the property,
    *   or null     if this cached value has been invalidated.
    */
  def getCachedPropertyAt(offset: Int): Value

  /**
    * Invalidate all cached node properties for the given node id
    */
  def invalidateCachedProperties(node: Long): Unit

  def copyWith(key: String, value: AnyValue): ExecutionContext
  def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext
  def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext
  def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext

  // Needed by legacy pattern matcher. Returns a map of all bound nodes/relationships in the context.
  // Entities that are only references (ids) are materialized with the provided materialization functions
  def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue]

  def isNull(key: String): Boolean
}

class MapExecutionContext(private val m: MutableMap[String, AnyValue], private var cachedProperties: MutableMap[CachedNodeProperty, Value] = null)
  extends ExecutionContext {

  override def copyTo(target: ExecutionContext, fromLongOffset: Int = 0, fromRefOffset: Int = 0, toLongOffset: Int = 0, toRefOffset: Int = 0): Unit = fail()

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = fail()

  override def copyCachedFrom(input: ExecutionContext): Unit = input match {
    case context : MapExecutionContext =>
      val oldCachedProperties = context.cachedProperties
      if (oldCachedProperties == null) cachedProperties = null
      else
      {
        cachedProperties = oldCachedProperties.clone()
        oldCachedProperties.foreach {
          case (CachedNodeProperty(varName,_),_) =>
            set(varName, context.getOrElse(varName, throw new IllegalStateException("The variable of a cached node property should be in the context.")))
        }
      }

    case _ =>
      fail()
  }

  override def setLongAt(offset: Int, value: Long): Unit = fail()
  override def getLongAt(offset: Int): Long = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()
  override def getRefAt(offset: Int): AnyValue = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a map context as a slotted context")

  override def get(key: String): Option[AnyValue] = m.get(key)

  override def iterator: Iterator[(String, AnyValue)] = m.iterator

  override def size: Int = m.size

  override def mergeWith(other: ExecutionContext, entityById: EntityById): Unit = other match {
    case otherMapCtx: MapExecutionContext =>
      m ++= otherMapCtx.m
      if (otherMapCtx.cachedProperties != null) {
        if (cachedProperties == null) {
          cachedProperties = otherMapCtx.cachedProperties.clone()
        } else {
          cachedProperties ++= otherMapCtx.cachedProperties
        }
      } else {
        //otherMapCtx.cachedProperties is null so do nothing
      }
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

  override def set(newEntries: Seq[(String, AnyValue)]): Unit =
    m ++= newEntries

  // This may seem silly but it has measurable impact in tight loops

  override def set(key: String, value: AnyValue): Unit =
    m.put(key, value)

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = {
    m.put(key1, value1)
    m.put(key2, value2)
  }

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): Unit = {
    m.put(key1, value1)
    m.put(key2, value2)
    m.put(key3, value3)
  }

  override def copyWith(key: String, value: AnyValue): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key, value)
    cloneFromMap(newMap)
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    cloneFromMap(newMap)
  }

  override def copyWith(key1: String, value1: AnyValue,
                        key2: String, value2: AnyValue,
                        key3: String, value3: AnyValue): ExecutionContext = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    newMap.put(key3, value3)
    cloneFromMap(newMap)
  }

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = {
    cloneFromMap(m.clone() ++ newEntries)
  }

  override def createClone(): ExecutionContext = cloneFromMap(m.clone())

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

  override def setCachedProperty(key: CachedNodeProperty, value: Value): Unit = {
    if (cachedProperties == null) {
      cachedProperties = MutableMap.empty
    }
    cachedProperties.put(key, value)
  }

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = fail()

  override def getCachedProperty(key: CachedNodeProperty): Value = {
    if (cachedProperties == null) {
      throw new NoSuchElementException("key not found: " + key)
    }
    cachedProperties(key)
  }

  override def getCachedPropertyAt(offset: Int): Value = fail()

  override def invalidateCachedProperties(node: Long): Unit = {
    if (cachedProperties != null)
      cachedProperties.keys.filter(cnp => apply(cnp.nodeVariableName) match {
        case n: VirtualNodeValue => n.id() == node
        case _ => false
      }).foreach(cnp => setCachedProperty(cnp, null))
  }

  private def cloneFromMap(newMap: MutableMap[String, AnyValue]): ExecutionContext = {
    val newCachedProperties = if (cachedProperties == null) null else cachedProperties.clone()
    new MapExecutionContext(newMap, newCachedProperties)
  }
}
