/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.Value

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

object InputDataStreamTestSupport {

  val RANDOM_VALUE_CONFIG: RandomValues.Configuration =
    new RandomValues.Configuration {
      override def stringMinLength = 0
      override def stringMaxLength = 50
      override def arrayMinLength = 0
      override def arrayMaxLength = 10
      override def maxCodePoint: Int = RandomValues.MAX_BMP_CODE_POINT
      override def minCodePoint: Int = Character.MIN_CODE_POINT
    }
}

/**
 * Test support for creating input data streams.
 */
trait InputDataStreamTestSupport {

  val NO_INPUT = new InputValues

  def randomValues(count: Int): Seq[Value] = {
    val random = RandomValues.create(InputDataStreamTestSupport.RANDOM_VALUE_CONFIG)
    (0 until count).map(_ => random.nextValue())
  }

  def inputValues(rows: Array[Any]*): InputValues =
    new InputValues().and(rows: _*)

  def batchedInputValues(batchSize: Int, rows: Array[Any]*): InputValues = {
    val input = new InputValues()
    rows.grouped(batchSize).foreach(batch => input.and(batch.toSeq: _*))
    input
  }

  // noinspection ScalaUnnecessaryParentheses
  def inputColumns(nBatches: Int, batchSize: Int, valueFunctions: (Int => Any)*): InputValues = {
    val input = new InputValues()
    for (batch <- 0 until nBatches) {
      val rows = for (row <- 0 until batchSize) yield valueFunctions.map(_(batch * batchSize + row)).toArray
      input.and(rows: _*)
    }
    input
  }

  def iteratorInput(batches: Iterator[Array[Any]]*): InputDataStream with IteratorProgress = {
    new IteratorInputStream(batches.map(_.map(_.map(ValueUtils.of))): _*) with IteratorProgress {
      override def nextCalls: Long = batches.head match {
        case b: IteratorProgress =>
          b.nextCalls
        case _ =>
          0L
      }
    }
  }

  def iteratorInputRaw(batches: Iterator[Array[AnyValue]]*): InputDataStream = {
    new IteratorInputStream(batches: _*)
  }

}

trait IteratorProgress {
  def nextCalls: Long
}

class InputValues() {
  val batches = new ArrayBuffer[IndexedSeq[Array[Any]]]

  def and(rows: Array[Any]*): InputValues = {
    batches += rows.toIndexedSeq
    this
  }

  def flatten: IndexedSeq[Array[Any]] =
    batches.flatten.toIndexedSeq

  def stream(): BufferInputStream = new BufferInputStream(batches.map(_.map(row => row.map(ValueUtils.asAnyValue))))
}

class BufferInputStream(data: ArrayBuffer[IndexedSeq[Array[AnyValue]]]) extends InputDataStream {
  private val batchIndex = new AtomicInteger(0)

  override def nextInputBatch(): InputCursor = {
    val i = batchIndex.getAndIncrement()
    if (i < data.size)
      new BufferInputCursor(data(i))
    else
      null
  }

  def hasMore: Boolean = batchIndex.get() < data.size
}

class BufferInputCursor(data: IndexedSeq[Array[AnyValue]]) extends InputCursor {
  private var i = -1

  override def next(): Boolean = {
    i += 1
    i < data.size
  }

  override def value(offset: Int): AnyValue =
    data(i)(offset)

  override def close(): Unit = {}
}

/**
 * Input data stream that streams data from multiple iterators, where each iterator corresponds to a batch.
 * It does not buffer any data.
 *
 * @param data the iterators
 */
class IteratorInputStream(data: Iterator[Array[AnyValue]]*) extends InputDataStream {
  private val batchIndex = new AtomicInteger(0)

  override def nextInputBatch(): InputCursor = {
    val i = batchIndex.getAndIncrement()
    if (i < data.size) {
      new IteratorInputCursor(data(i))
    } else {
      null
    }
  }
}

class IteratorInputCursor(data: Iterator[Array[AnyValue]]) extends InputCursor {
  private var _next: Array[AnyValue] = _

  override def next(): Boolean = {
    if (data.hasNext) {
      _next = data.next()
      true
    } else {
      _next = null
      false
    }
  }

  override def value(offset: Int): AnyValue = _next(offset)

  override def close(): Unit = {}
}
