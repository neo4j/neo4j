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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.values.AnyValue

case class EagerRegisterPipe(source: Pipe, pipelineInformation: PipelineInformation)(val id: Id = new Id)
  extends PipeWithSource(source) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val longColumnCount = pipelineInformation.numberOfLongs
    val longRowsBuilder = new ResizableLongArray(longColumnCount * 64, 2)
    val refColumnCount = pipelineInformation.numberOfReferences
    val refRowsBuilder = new ResizableRefArray(refColumnCount * 64, 2)
    input.foreach(ctx => {
      if (longColumnCount > 0) {
        longRowsBuilder ++= ctx.longs()
      }
      if (refColumnCount > 0) {
        refRowsBuilder ++= ctx.refs()
      }
    })

    new Iterator[ExecutionContext] {
      private var longIndex = 0
      private var refIndex = 0
      private val longRows = longRowsBuilder.array
      private val longRowsLength = longRowsBuilder.length
      private val refRows = refRowsBuilder.array
      private val refRowsLength = refRowsBuilder.length

      override def hasNext: Boolean = longIndex < longRowsLength || refIndex < refRowsLength

      override def next(): ExecutionContext = {
        val row = new ExecutionContext {
          private val refIndexSnapshot = refIndex
          private val longIndexSnapshot = longIndex

          override def setLongAt(offset: Int, value: Long): Unit = longRows(offset + longIndexSnapshot) = value

          override def setRefAt(offset: Int, value: AnyValue): Unit = refRows(offset + refIndexSnapshot) = value

          // when called from PrimitiveExecutionContext#copyFrom the array creation is redundant, it occurs again in
          // PrimitiveExecutionContext#copyFrom
          // adding something like the following would result in fewer array creations:
          //  > fillLongs(to: Array[Long]) = System.arraycopy(longRows, longIndexSnapshot, to, 0, longColumnCount)
          override def longs(): Array[Long] = {
            val longs = new Array[Long](longColumnCount)
            System.arraycopy(longRows, longIndexSnapshot, longs, 0, longColumnCount)
            longs
          }

          override def refs(): Array[AnyValue] = {
            val refs = new Array[AnyValue](refColumnCount)
            System.arraycopy(refRows, refIndexSnapshot, refs, 0, refColumnCount)
            refs
          }

          override def getRefAt(offset: Int): AnyValue = refRows(offset + refIndexSnapshot)

          override def copyFrom(input: ExecutionContext): Unit = fail()

          override def getLongAt(offset: Int): Long = longRows(offset + longIndexSnapshot)

          override def newWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

          override def createClone(): ExecutionContext = fail()

          override def newWith1(key1: String, value1: AnyValue): ExecutionContext = fail()

          override def newWith2(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

          override def newWith3(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

          override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

          override def +=(kv: (String, AnyValue)): this.type = fail()

          override def -=(key: String): this.type = fail()

          override def iterator: Iterator[(String, AnyValue)] = fail()

          override def get(key: String): Option[AnyValue] = fail()

          private def fail(): Nothing =
            throw new InternalException(s"Not supported in anonymous ${classOf[EagerRegisterPipe]} execution context")
        }
        longIndex += longColumnCount
        refIndex += refColumnCount

        row
      }
    }
  }

  private class ResizableLongArray(initialSize: Int, growth: Int) extends ResizableArray[Long](initialSize, growth) {
    override protected def newArray(size: Int): Array[Long] = new Array[Long](size)
  }

  private class ResizableRefArray(initialSize: Int, growth: Int) extends ResizableArray[AnyValue](initialSize, growth) {
    override protected def newArray(size: Int): Array[AnyValue] = new Array[AnyValue](size)
  }

  private abstract class ResizableArray[T](initialSize: Int, growth: Int) {
    var array: Array[T] = newArray(initialSize)

    var length: Int = 0

    def ++=(elements: Array[T]): Unit = {
      while (length + elements.length > array.length)
        grow()
      System.arraycopy(elements, 0, array, length, elements.length)
      length = length + elements.length
    }

    protected def newArray(size: Int): Array[T]

    // NOTE: current impl doesn't reuse space of old array. Instead, it creates bigger array & copies old elements to it
    private def grow(): Unit = {
      val largerArray = newArray(array.length * growth)
      System.arraycopy(array, 0, largerArray, 0, array.length)
      array = largerArray
    }
  }

}
