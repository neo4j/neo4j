/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryLimitExceededException
import org.neo4j.memory.MemoryPools
import org.neo4j.values.storable.Values

class BoundedQueryMemoryTrackerTest extends CypherFunSuite {

  case class IMem(i: Int) extends Measurable {
    override def estimatedHeapUsage: Long = i
  }

  case class TestMemoryTracker(maxBytes: Long = Long.MaxValue) extends LocalMemoryTracker(MemoryPools.NO_TRACKING, maxBytes, 0, null)

  test("Tracks overall memory high water mark with bytes") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    // When
    tracker.allocated(10, 0)
    tracker.allocated(5, 0)
    tracker.deallocated(6, 0)
    tracker.allocated(4, 0)
    // Then
    tracker.totalAllocatedMemory should be(15L)
    tracker.maxMemoryOfOperator(0) should be(15L)
  }

  test("Tracks overall memory high water mark with AnyValues") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val v1 = Values.intValue(5)
    val v2 = Values.booleanValue(true)
    val v3 = Values.stringValue("foo")
    val v4 = Values.booleanValue(false)
    // When
    tracker.allocated(v1, 0)
    tracker.allocated(v2, 0)
    tracker.deallocated(v3, 0)
    tracker.allocated(v4, 0)
    // Then
    val expected = Math.max(v1.estimatedHeapUsage() + v2.estimatedHeapUsage(), v1.estimatedHeapUsage() + v2.estimatedHeapUsage() - v3.estimatedHeapUsage() + v4.estimatedHeapUsage())
    tracker.totalAllocatedMemory should be(expected)
    tracker.maxMemoryOfOperator(0) should be(expected)
  }

  test("Tracks overall memory high water mark with WithHeapUsageEstimation") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    // When
    tracker.allocated(IMem(10), 0)
    tracker.allocated(IMem(5), 0)
    tracker.deallocated(IMem(6), 0)
    tracker.allocated(IMem(4), 0)
    // Then
    tracker.totalAllocatedMemory should be(15L)
    tracker.maxMemoryOfOperator(0) should be(15L)
  }

  test("Iterator tracks memory") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val iterator = tracker.memoryTrackingIterator(Iterator(IMem(1), IMem(6), IMem(2), IMem(100)), 0)
    // When
    iterator.next() // 1
    tracker.allocated(10, 0) // 11
    iterator.next() // 17
    tracker.deallocated(3, 0) // 14
    iterator.next() // 16
    // Then
    tracker.totalAllocatedMemory should be(17L)
    tracker.maxMemoryOfOperator(0) should be(17L)
  }

  test("Throws exception if memory exceeds threshold") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker(20))
    // When
    tracker.allocated(10, 0)
    tracker.allocated(5, 0)
    tracker.deallocated(6, 0)
    tracker.allocated(9, 0)
    // Then
    a[MemoryLimitExceededException] should be thrownBy tracker.allocated(3, 0)
  }

  test("Tracks individual memory per operator plus overall") {
    // Given
    val tracker = BoundedQueryMemoryTracker(TestMemoryTracker())
    val iterator0 = tracker.memoryTrackingIterator(Iterator(IMem(1), IMem(6), IMem(8)), 0)
    val iterator1 = tracker.memoryTrackingIterator(Iterator(IMem(2), IMem(4), IMem(7)), 1)
    val iterator2 = tracker.memoryTrackingIterator(Iterator(IMem(3), IMem(13), IMem(2)), 2)
    // When
    iterator0.next() // [1, 0, 0] / 1
    tracker.allocated(10, 2) // [1, 0, 10] / 11
    iterator1.next() // [1, 2, 10] / 13
    iterator0.next() // [7, 2, 10] / 19
    tracker.deallocated(5, 0) // [2, 2, 10] / 14
    iterator2.next() // [2, 2, 13] / 17
    iterator2.next() // [2, 2, 26] / 30
    iterator2.next() // [2, 2, 28] / 32
    tracker.deallocated(21, 2) // [2, 2, 7] / 11
    iterator0.next() // [10, 2, 7] / 19
    iterator1.next() // [10, 6, 7] / 23
    tracker.deallocated(2, 1) // [10, 4, 7] / 21
    iterator1.next() // [10, 11, 7] / 28
    // Then
    tracker.totalAllocatedMemory should be(32L)
    tracker.maxMemoryOfOperator(0) should be(10L)
    tracker.maxMemoryOfOperator(1) should be(11L)
    tracker.maxMemoryOfOperator(2) should be(28L)
  }
}
