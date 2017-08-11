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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

import scala.collection.mutable.ArrayBuffer

case class EagerRegisterPipe(source: Pipe, pipelineInformation: PipelineInformation)(val id: Id = new Id)
  extends PipeWithSource(source) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val primitiveColumnCount = pipelineInformation.numberOfLongs
    val primitiveRows: ArrayBuffer[Long] = new ArrayBuffer[Long]()
    val refColumnCount = pipelineInformation.numberOfReferences
    val refRows = new ArrayBuffer[Any]()
    if (primitiveColumnCount > 0) {
      input.foreach(ctx => {
        if (primitiveColumnCount > 0) {
          primitiveRows ++= ctx.longs()
        }
        if (refColumnCount > 0) {
          refRows ++= ctx.refs()
        }
      })
    }

    new Iterator[ExecutionContext] {
      private var primitiveIndex = 0
      private var refIndex = 0
      private val primitiveArrayLength = primitiveRows.length
      private val refArrayLength = refRows.length

      override def hasNext: Boolean = primitiveIndex < primitiveArrayLength || refIndex < refArrayLength

      override def next(): ExecutionContext = {
        val row = new ExecutionContext {
          private val globalRefOffset = refIndex
          private val globalPrimitiveOffset = primitiveIndex

          override def setLongAt(offset: Int, value: Long): Unit = primitiveRows(offset + globalPrimitiveOffset) = value

          override def setRefAt(offset: Int, value: Any): Unit = refRows(offset + globalRefOffset) = value

          // if this is called from somewhere like PrimitiveExecutionContext#copyFrom the array copy is redundant
          // adding something like the following would result in 1 fewer array copies per row:
          //  > fillLongs(to: Array[Long]) = primitiveRows.copyToArray(to, globalPrimitiveOffset, primitiveColumnCount)
          override def longs(): Array[Long] = {
            val longsArray = new Array[Long](primitiveColumnCount)
            primitiveRows.copyToArray(longsArray, globalPrimitiveOffset, primitiveColumnCount)
            longsArray
          }

          override def refs(): Array[Any] = {
            val refsArray = new Array[Any](refColumnCount)
            refRows.copyToArray(refsArray, globalRefOffset, refColumnCount)
            refsArray
          }

          override def getRefAt(offset: Int): Any = refRows(offset + globalRefOffset)

          override def copyFrom(input: ExecutionContext): Unit = fail()

          override def getLongAt(offset: Int): Long = primitiveRows(offset + globalPrimitiveOffset)

          override def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = fail()

          override def createClone(): ExecutionContext = fail()

          override def newWith1(key1: String, value1: Any): ExecutionContext = fail()

          override def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = fail()

          override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = fail()

          override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

          override def +=(kv: (String, Any)): this.type = fail()

          override def -=(key: String): this.type = fail()

          override def iterator: Iterator[(String, Any)] = fail()

          override def get(key: String): Option[Any] = fail()

          private def fail(): Nothing =
            throw new InternalException(s"Not supported in anonymous ${classOf[EagerRegisterPipe]} execution context")
        }
        primitiveIndex += primitiveColumnCount
        refIndex += refColumnCount

        row
      }
    }
  }

}
