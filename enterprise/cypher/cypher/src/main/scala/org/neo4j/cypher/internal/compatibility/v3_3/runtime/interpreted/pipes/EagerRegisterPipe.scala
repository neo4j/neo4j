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

import scala.collection.mutable


case class EagerRegisterPipe(source: Pipe, pipelineInformation: PipelineInformation)(val id: Id = new Id)
  extends PipeWithSource(source) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val columnCount = pipelineInformation.numberOfLongs
    val arrayBuilder = new mutable.ArrayBuilder.ofLong

    input.foreach( ctx =>
      // TODO find way to copy from ctx to array in 1 operation, e.g., using arraycopy
      ctx.longs().foreach(l => arrayBuilder += l)
    )

    val rows: Array[Long] = arrayBuilder.result()

    new Iterator[ExecutionContext] {
      var index = 0
      val arrayLength = rows.length

      override def hasNext: Boolean = index < arrayLength

      override def next(): ExecutionContext = {
        val row = new ExecutionContext {
          override def setLongAt(offset: Int, value: Long): Unit = ???

          override def setRefAt(offset: Int, value: Any): Unit = ???

          override def longs(): Array[Long] = ???

          override def getRefAt(offset: Int): Any = ???

          override def copyFrom(input: ExecutionContext): Unit = ???

          override def getLongAt(offset: Int): Long = rows(offset + index)

          override def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = ???

          override def createClone(): ExecutionContext = ???

          override def newWith1(key1: String, value1: Any): ExecutionContext = ???

          override def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = ???

          override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = ???

          override def mergeWith(other: ExecutionContext): ExecutionContext = ???

          override def +=(kv: (String, Any)): this.type = ???

          override def -=(key: String): this.type = ???

          override def iterator: Iterator[(String, Any)] = ???

          override def get(key: String): Option[Any] = ???
        }
        index += columnCount

        row
      }
    }

   //     System.arraycopy(currentRow.rows, 0, currentBucket.rows, currentIndex * columnCount, columnCount)

  }

}
