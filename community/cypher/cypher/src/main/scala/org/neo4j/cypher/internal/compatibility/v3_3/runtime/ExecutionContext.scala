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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.MutableMaps
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.{Iterator, immutable}

object ExecutionContext {
  def empty: ExecutionContext = apply()

  def from(x: (String, Any)*): ExecutionContext = apply().newWith(x)

  def apply(m: MutableMap[String, Any] = MutableMaps.empty) = MapExecutionContext(m)
}

trait ExecutionContext extends MutableMap[String, Any] {
  def copyFrom(input: ExecutionContext): Unit
  def setLongAt(offset: Int, value: Long): Unit
  def getLongAt(offset: Int): Long

  def newWith(newEntries: Seq[(String, Any)]): ExecutionContext
  def newWith1(key1: String, value1: Any): ExecutionContext
  def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext
  def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext
  def mergeWith(other: ExecutionContext): ExecutionContext
  def createClone(): ExecutionContext
}

case class MapExecutionContext(m: MutableMap[String, Any])
  extends ExecutionContext {

  override def copyFrom(input: ExecutionContext): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()
  override def getLongAt(offset: Int): Long = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a map context as a primitive context")
  def setRefAt(offset: Int, value: Any): Unit = fail()
  def getRefAt(offset: Int): Any = fail()

  override def get(key: String): Option[Any] = m.get(key)

  override def iterator: Iterator[(String, Any)] = m.iterator

  override def size: Int = m.size

  override def mergeWith(other: ExecutionContext): ExecutionContext = other match {
    case MapExecutionContext(otherMap) => copy(m = m ++ otherMap)
    case _ => fail()
  }

  override def foreach[U](f: ((String, Any)) => U) {
    m.foreach(f)
  }

  override def +=(kv: (String, Any)) = {
    m += kv
    this
  }

  override def toMap[T, U](implicit ev: (String, Any) <:< (T, U)): immutable.Map[T, U] = m.toMap(ev)

  def newWith(newEntries: Seq[(String, Any)]) =
    createWithNewMap(m.clone() ++= newEntries)

  // This may seem silly but it has measurable impact in tight loops

  override def newWith1(key1: String, value1: Any) = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    createWithNewMap(newMap)
  }

  override def newWith2(key1: String, value1: Any, key2: String, value2: Any) = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    createWithNewMap(newMap)
  }

  override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any) = {
    val newMap = m.clone()
    newMap.put(key1, value1)
    newMap.put(key2, value2)
    newMap.put(key3, value3)
    createWithNewMap(newMap)
  }

  override def createClone(): ExecutionContext = createWithNewMap(m.clone())

  protected def createWithNewMap(newMap: MutableMap[String, Any]): this.type = {
    copy(m = newMap).asInstanceOf[this.type]
  }

  override def -=(key: String): this.type = {
    m.remove(key)
    this
  }
}
