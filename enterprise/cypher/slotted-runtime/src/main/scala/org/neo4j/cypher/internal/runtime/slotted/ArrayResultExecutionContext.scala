/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue

import scala.collection.mutable

case class ArrayResultExecutionContextFactory(columns: Seq[(String, Expression)]) {
  private val columnExpressionArray = columns.map(_._2).toArray
  private val columnArraySize = columnExpressionArray.size
  private val columnIndexMap = {
    val m = new mutable.OpenHashMap[String, Int](columns.length)
    var index = 0
    columns.foreach {
      case (name, exp) => m.put(name, index)
      index += 1
    }
    m
  }

  def newResult(context: ExecutionContext, state: QueryState): ArrayResultExecutionContext = {
    val result = allocateExecutionContext

    // Apply the expressions that materializes the result values and fill in the result array
    val resultArray = result.resultArray
    var index = 0
    while (index < columnArraySize) {
      resultArray(index) = columnExpressionArray(index).apply(context, state)
      index += 1
    }
    result
  }

  //---------------------------------------------------------------------------
  // Instance cache of size 1. Reuses the last created ArrayResultExecutionContext
  private var freeExecutionContextInstance: ArrayResultExecutionContext = null

  private def allocateExecutionContext: ArrayResultExecutionContext = {
    if (freeExecutionContextInstance != null) {
      val context = freeExecutionContextInstance
      freeExecutionContextInstance = null
      context
    }
    else {
      createNewExecutionContext
    }
  }

  def releaseExecutionContext(executionContext: ArrayResultExecutionContext) = {
    freeExecutionContextInstance = executionContext
  }

  private def createNewExecutionContext: ArrayResultExecutionContext = {
    val resultArray = new Array[AnyValue](columnArraySize)
    ArrayResultExecutionContext(resultArray, columnIndexMap, this)
  }
  //---------------------------------------------------------------------------
}

case class ArrayResultExecutionContext(resultArray: Array[AnyValue],
                                       columnIndexMap: scala.collection.Map[String, Int],
                                       factory: ArrayResultExecutionContextFactory)
  extends ExecutionContext with QueryResult.Record {

  override def release(): Unit = factory.releaseExecutionContext(this)

  override def get(key: String): Option[AnyValue] = {
    columnIndexMap.get(key) match {
      case Some(index) => Some(resultArray(index))
      case _=> None
    }
  }

  override def iterator: Iterator[(String, AnyValue)] = {
    columnIndexMap.iterator.map {
      case (name, index) =>
        (name, resultArray(index))
    }
  }

  override def fields(): Array[AnyValue] = {
    resultArray
  }

  override def size: Int = resultArray.size

  //---------------------------------------------------------------------------
  // This is an ExecutionContext by name only and does not support the full API
  // The methods below should never be called on a produced result
  private def fail(): Nothing = throw new InternalException("Tried using a result context as an execution context")

  override def copyTo(target: ExecutionContext, fromLongOffset: Int, fromRefOffset: Int, toLongOffset: Int, toRefOffset: Int): Unit = fail()

  override def copyFrom(input: ExecutionContext, nLongs: Int, nRefs: Int): Unit = fail()

  override def setLongAt(offset: Int, value: Long): Unit = fail()

  override def getLongAt(offset: Int): Long = fail()

  override def longs(): Array[Long] = fail()

  override def setRefAt(offset: Int, value: AnyValue): Unit = fail()

  override def getRefAt(offset: Int): AnyValue = fail()

  override def refs(): Array[AnyValue] = fail()

  override def set(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def set(key: String, value: AnyValue): ExecutionContext = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def mergeWith(other: ExecutionContext): ExecutionContext = fail()

  override def createClone(): ExecutionContext = fail()

  override def copyWith(key: String, value: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): ExecutionContext = fail()

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue, key3: String, value3: AnyValue): ExecutionContext = fail()

  override def copyWith(newEntries: Seq[(String, AnyValue)]): ExecutionContext = fail()

  override def boundEntities(materializeNode: Long => AnyValue, materializeRelationship: Long => AnyValue): Map[String, AnyValue] = fail()

  override def isNull(key: String): Boolean = fail()

  override def +=(kv: (String, AnyValue)): ArrayResultExecutionContext.this.type = fail()

  override def -=(key: String): ArrayResultExecutionContext.this.type = fail()
}
