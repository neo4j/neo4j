/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9

import mutation.UpdateAction
import pipes.MutableMaps
import scala.Predef._
import collection.{immutable, Iterator}
import collection.mutable.{Queue, Map => MutableMap}

object ExecutionContext {
  def empty = new ExecutionContext()

  def from(x: (String, Any)*) = new ExecutionContext().newWith(x)
}

case class ExecutionContext(m: MutableMap[String, Any] = MutableMaps.empty,
                            mutationCommands: Queue[UpdateAction] = Queue.empty)
  extends MutableMap[String, Any] {
  def get(key: String): Option[Any] = m.get(key)

  def iterator: Iterator[(String, Any)] = m.iterator

  override def size = m.size

  def ++(other: ExecutionContext): ExecutionContext = copy(m = m ++ other.m)

  override def foreach[U](f: ((String, Any)) => U) {
    m.foreach(f)
  }

  def +=(kv: (String, Any)) = {
    m += kv
    this
  }

  def -=(key: String) = {
    m -= key
    this
  }

  override def toMap[T, U](implicit ev: (String, Any) <:< (T, U)): immutable.Map[T, U] = m.toMap(ev)

  def newWith(newEntries: Seq[(String, Any)]) =
    createWithNewMap(MutableMaps.create(this.m) ++= newEntries)

  def newWith(newEntries: scala.collection.Map[String, Any]) =
    createWithNewMap(MutableMaps.create(this.m) ++= newEntries)

  def newFrom(newEntries: Seq[(String, Any)]) =
    createWithNewMap(MutableMaps.create(newEntries: _*))

  def newFrom(newEntries: scala.collection.Map[String, Any]) =
    createWithNewMap(MutableMaps.create(newEntries))

  def newWith(newEntry: (String, Any)) =
    createWithNewMap(MutableMaps.create(this.m) += newEntry)

  override def clone(): ExecutionContext = newFrom(m)

  protected def createWithNewMap(newMap: MutableMap[String, Any]) = {
    copy(m = newMap)
  }
}

