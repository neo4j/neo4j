/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

case class PrimitiveExecutionContext(pipeline: PipelineInformation) extends ExecutionContext {
  def copyFrom(input: ExecutionContext): Unit = input match {
    case other@PrimitiveExecutionContext(otherPipeline) =>
      if (otherPipeline.numberOfLongs > pipeline.numberOfLongs)
        throw new InternalException("Tried to copy more data into less.")
      else
        System.arraycopy(other.longs, 0, longs, 0, otherPipeline.numberOfLongs)

    case _ => fail()
  }

  private val longs = new Array[Long](pipeline.numberOfLongs)

  def setLongAt(offset: Int, value: Long): Unit = longs(offset) = value

  def getLongAt(offset: Int): Long = longs(offset)

  override def +=(kv: (String, Any)) = fail()

  override def -=(key: String) = fail()

  override def get(key: String) = fail()

  override def iterator = fail()

  private def fail(): Nothing = throw new InternalException("Tried using a primitive context as a map")

  override def newWith1(key1: String, value1: Any): ExecutionContext = fail()

  override def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = fail()

  override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = fail()

  override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

  override def createClone(): ExecutionContext = fail()

  override def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = fail()
}
