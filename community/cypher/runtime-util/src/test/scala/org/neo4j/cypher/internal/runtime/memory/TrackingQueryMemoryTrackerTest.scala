/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.cypher.internal.runtime.GrowingArray
import org.neo4j.cypher.internal.runtime.GrowingArray.DEFAULT_SIZE
import org.neo4j.cypher.internal.runtime.memory.TrackingQueryMemoryTrackerTest.DEFAULT_SIZE_OF_GROWING_ARRAY
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.memory.MemoryPools
import org.neo4j.values.storable.Values

object TrackingQueryMemoryTrackerTest {
  val DEFAULT_SIZE_OF_GROWING_ARRAY: Long = shallowSizeOfObjectArray(DEFAULT_SIZE) + GrowingArray.SHALLOW_SIZE
}

class TrackingQueryMemoryTrackerTest extends CypherFunSuite {

  case class TestMemoryTracker(maxBytes: Long = Long.MaxValue) extends LocalMemoryTracker(MemoryPools.NO_TRACKING, maxBytes, 0, null)

  private val sizeOfGrowingArray: Long = DEFAULT_SIZE_OF_GROWING_ARRAY

  test("Tracks overall memory high water mark: Single transaction, single operator") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val txMemoryTracker = TestMemoryTracker()
    val memoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider(txMemoryTracker)
    val operator0Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(0)

    // When
    operator0Tracker.allocateHeap(10)
    operator0Tracker.allocateHeap(5)
    operator0Tracker.releaseHeap(6)
    operator0Tracker.allocateHeap(4)

    // Then
    queryMemoryTracker.heapHighWaterMark should be(15L + sizeOfGrowingArray)
    txMemoryTracker.heapHighWaterMark() should be(15L)
    queryMemoryTracker.heapHighWaterMarkOfOperator(0) should be(15L)
  }

  test("Tracks overall memory high water mark with AnyValues: Single transaction, single operator") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val txMemoryTracker = TestMemoryTracker()
    val memoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider(txMemoryTracker)
    val operator0Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(0)

    val v1 = Values.intValue(5)
    val v2 = Values.booleanValue(true)
    val v3 = Values.stringValue("foo")
    val v4 = Values.booleanValue(false)

    // When
    operator0Tracker.allocateHeap(v1.estimatedHeapUsage)
    operator0Tracker.allocateHeap(v3.estimatedHeapUsage)
    operator0Tracker.releaseHeap(v2.estimatedHeapUsage)
    operator0Tracker.allocateHeap(v4.estimatedHeapUsage)

    // Then
    val expected = Math.max(v1.estimatedHeapUsage() + v3.estimatedHeapUsage(), v1.estimatedHeapUsage() + v3.estimatedHeapUsage() - v2.estimatedHeapUsage() + v4.estimatedHeapUsage())
    queryMemoryTracker.heapHighWaterMark should be(expected + sizeOfGrowingArray)
    txMemoryTracker.heapHighWaterMark() should be(expected)
    queryMemoryTracker.heapHighWaterMarkOfOperator(0) should be(expected)
  }

  test("Throws exception if operator allocations exceed threshold: Single transaction, single operator") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val txMemoryTracker = TestMemoryTracker(20)
    val memoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider(txMemoryTracker)
    val operator0Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(0)

    // When
    operator0Tracker.allocateHeap(10)
    operator0Tracker.allocateHeap(5)
    operator0Tracker.releaseHeap(6)
    operator0Tracker.allocateHeap(9)

    // Then
    a[MemoryLimitExceededException] should be thrownBy operator0Tracker.allocateHeap(3)
  }

  test("Tracks individual memory per operator plus overall: Single transaction, multiple operators") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val txMemoryTracker = TestMemoryTracker()
    val memoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider(txMemoryTracker)
    val operator0Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(0)
    val operator1Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(1)
    val operator2Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(2)

    // When. Format is [op0, op1, op2] / query
    operator0Tracker.allocateHeap(1)  // [ 1,  0,  0] /  1
    operator2Tracker.allocateHeap(10) // [ 1,  0, 10] / 11
    operator1Tracker.allocateHeap(2)  // [ 1,  2, 10] / 13
    operator0Tracker.allocateHeap(6)  // [ 7,  2, 10] / 19
    operator0Tracker.releaseHeap(5)   // [ 2,  2, 10] / 14
    operator2Tracker.allocateHeap(3)  // [ 2,  2, 13] / 17
    operator2Tracker.allocateHeap(13) // [ 2,  2, 26] / 30
    operator2Tracker.allocateHeap(2)  // [ 2,  2, 28] / 32
    operator2Tracker.releaseHeap(21)  // [ 2,  2,  7] / 11
    operator0Tracker.allocateHeap(8)  // [10,  2,  7] / 19
    operator1Tracker.allocateHeap(4)  // [10,  6,  7] / 23
    operator1Tracker.releaseHeap(2)   // [10,  4,  7] / 21
    operator1Tracker.allocateHeap(7)  // [10, 11,  7] / 28

    // Then
    queryMemoryTracker.heapHighWaterMarkOfOperator(0) should be(10L)
    queryMemoryTracker.heapHighWaterMarkOfOperator(1) should be(11L)
    queryMemoryTracker.heapHighWaterMarkOfOperator(2) should be(28L)
    txMemoryTracker.heapHighWaterMark() should be(32L)
    queryMemoryTracker.heapHighWaterMark should be(32L + sizeOfGrowingArray)
  }

  test("Throws exception if operator allocations exceed threshold: Single transaction, multiple operators") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val txMemoryTracker = TestMemoryTracker(20)
    val memoryTrackerForOperatorProvider = queryMemoryTracker.newMemoryTrackerForOperatorProvider(txMemoryTracker)
    val operator0Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(0)
    val operator1Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(1)
    val operator2Tracker = memoryTrackerForOperatorProvider.memoryTrackerForOperator(2)

    // When
    operator0Tracker.allocateHeap(10)
    operator1Tracker.allocateHeap(5)
    operator0Tracker.releaseHeap(6)
    operator1Tracker.allocateHeap(9)

    // Then
    a[MemoryLimitExceededException] should be thrownBy operator2Tracker.allocateHeap(3)
  }

  test("Tracks overall memory high water mark and memory per transaction: Multiple transactions, single operator") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val tx0MemoryTracker = TestMemoryTracker()
    val tx1MemoryTracker = TestMemoryTracker()
    val memoryTrackerForOperatorProvider0 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx0MemoryTracker)
    val memoryTrackerForOperatorProvider1 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx1MemoryTracker)
    val operator0Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(0)
    val operator0Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(0)

    // When
    operator0Tx0Tracker.allocateHeap(10)
    operator0Tx1Tracker.allocateHeap(5)
    operator0Tx0Tracker.releaseHeap(6)
    operator0Tx1Tracker.allocateHeap(4)

    // Then
    queryMemoryTracker.heapHighWaterMark should be(15L + sizeOfGrowingArray)
    queryMemoryTracker.heapHighWaterMarkOfOperator(0) should be(15L)
    tx0MemoryTracker.heapHighWaterMark() should be(10L)
    tx1MemoryTracker.heapHighWaterMark() should be(9L)
  }

  test("Throws exception if operator allocations exceed threshold: Multiple transactions, single operator") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val tx0MemoryTracker = TestMemoryTracker(20)
    val tx1MemoryTracker = TestMemoryTracker(15)
    val memoryTrackerForOperatorProvider0 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx0MemoryTracker)
    val memoryTrackerForOperatorProvider1 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx1MemoryTracker)
    val operator0Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(0)
    val operator0Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(0)

    // When
    operator0Tx0Tracker.allocateHeap(10)
    operator0Tx0Tracker.allocateHeap(5)
    operator0Tx0Tracker.releaseHeap(6)
    operator0Tx0Tracker.allocateHeap(9)

    // Then
    a[MemoryLimitExceededException] should be thrownBy operator0Tx0Tracker.allocateHeap(3)

    // And when
    operator0Tx1Tracker.allocateHeap(10)
    operator0Tx1Tracker.allocateHeap(4)
    operator0Tx1Tracker.releaseHeap(6)

    // And then
    a[MemoryLimitExceededException] should be thrownBy operator0Tx1Tracker.allocateHeap(9)
  }

  test("Throws exception if operator allocations exceed threshold: Multiple transactions, multiple operators") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val tx0MemoryTracker = TestMemoryTracker(20)
    val tx1MemoryTracker = TestMemoryTracker(15)
    val memoryTrackerForOperatorProvider0 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx0MemoryTracker)
    val memoryTrackerForOperatorProvider1 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx1MemoryTracker)
    val operator0Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(0)
    val operator0Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(0)
    val operator1Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(1)
    val operator1Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(1)

    // When
    operator0Tx0Tracker.allocateHeap(10)
    operator1Tx0Tracker.allocateHeap(5)
    operator0Tx0Tracker.releaseHeap(6)
    operator1Tx0Tracker.allocateHeap(9)

    // Then
    a[MemoryLimitExceededException] should be thrownBy operator0Tx0Tracker.allocateHeap(3)

    // And when
    operator0Tx1Tracker.allocateHeap(10)
    operator1Tx1Tracker.allocateHeap(4)
    operator0Tx1Tracker.releaseHeap(6)

    // And then
    a[MemoryLimitExceededException] should be thrownBy operator1Tx1Tracker.allocateHeap(9)
  }

  test("Tracks individual memory per operator and per transaction plus overall: Multiple transactions, multiple operators") {
    // Given
    val queryMemoryTracker = new TrackingQueryMemoryTracker()
    val tx0MemoryTracker = TestMemoryTracker()
    val tx1MemoryTracker = TestMemoryTracker()
    val memoryTrackerForOperatorProvider0 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx0MemoryTracker)
    val memoryTrackerForOperatorProvider1 = queryMemoryTracker.newMemoryTrackerForOperatorProvider(tx1MemoryTracker)
    val operator0Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(0)
    val operator1Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(1)
    val operator2Tx0Tracker = memoryTrackerForOperatorProvider0.memoryTrackerForOperator(2)
    val operator0Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(0)
    val operator1Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(1)
    val operator2Tx1Tracker = memoryTrackerForOperatorProvider1.memoryTrackerForOperator(2)

    // When. Format is [op0, op1, op2] / [tx0, tx1] / query
    operator0Tx0Tracker.allocateHeap(1)  // [ 1,  0,  0] / [ 1,  0] /  1
    operator2Tx1Tracker.allocateHeap(10) // [ 1,  0, 10] / [ 1, 10] / 11
    operator1Tx1Tracker.allocateHeap(2)  // [ 1,  2, 10] / [ 1, 12] / 13
    operator0Tx1Tracker.allocateHeap(6)  // [ 7,  2, 10] / [ 1, 18] / 19
    operator0Tx1Tracker.releaseHeap(5)   // [ 2,  2, 10] / [ 1, 13] / 14
    operator2Tx0Tracker.allocateHeap(3)  // [ 2,  2, 13] / [ 4, 13] / 17
    operator2Tx0Tracker.allocateHeap(13) // [ 2,  2, 26] / [17, 13] / 30
    operator2Tx1Tracker.allocateHeap(2)  // [ 2,  2, 28] / [17, 15] / 32
    operator2Tx0Tracker.releaseHeap(14)  // [ 2,  2, 14] / [ 3, 15] / 18
    operator0Tx1Tracker.allocateHeap(8)  // [10,  2, 14] / [ 3, 23] / 26
    operator1Tx0Tracker.allocateHeap(4)  // [10,  6, 14] / [ 7, 23] / 30
    operator1Tx0Tracker.releaseHeap(2)   // [10,  4, 14] / [ 5, 23] / 28
    operator1Tx1Tracker.allocateHeap(7)  // [10, 11, 14] / [ 5, 30] / 35

    // Then
    queryMemoryTracker.heapHighWaterMarkOfOperator(0) should be(10L)
    queryMemoryTracker.heapHighWaterMarkOfOperator(1) should be(11L)
    queryMemoryTracker.heapHighWaterMarkOfOperator(2) should be(28L)
    tx0MemoryTracker.heapHighWaterMark() should be(17L)
    tx1MemoryTracker.heapHighWaterMark() should be(30L)
    queryMemoryTracker.heapHighWaterMark should be(35 + sizeOfGrowingArray)
  }
}
