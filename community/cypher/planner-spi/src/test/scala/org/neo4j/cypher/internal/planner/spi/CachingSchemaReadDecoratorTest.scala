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
package org.neo4j.cypher.internal.planner.spi

import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.kernel.api.index.IndexSample

class CachingSchemaReadDecoratorTest extends CypherFunSuite {

  test("should cache index sample") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val anotherIndexDescriptor = mock[IndexDescriptor]
    val expectedIndexSample = new IndexSample(1000, 100, 1000)
    val anotherIndexSample = new IndexSample(2000, 200, 2000)
    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(expectedIndexSample)
    when(mockDelegate.indexSample(anotherIndexDescriptor)).thenReturn(anotherIndexSample)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // given I run the call once
    cachingDecorator.indexSample(indexDescriptor) shouldEqual expectedIndexSample
    // then the delegate should have been called
    verify(mockDelegate).indexSample(indexDescriptor)
    // when I run the call again
    cachingDecorator.indexSample(indexDescriptor) shouldEqual expectedIndexSample
    verifyNoMoreInteractions(mockDelegate)

    // when I use another index descriptor
    cachingDecorator.indexSample(anotherIndexDescriptor)
    verify(mockDelegate).indexSample(anotherIndexDescriptor)
  }

  test("should cache index state if non-locking") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val anotherIndexDescriptor = mock[IndexDescriptor]
    val expectedIndexState = org.neo4j.internal.kernel.api.InternalIndexState.ONLINE
    when(mockDelegate.indexGetStateNonLocking(indexDescriptor)).thenReturn(expectedIndexState)
    when(
      mockDelegate.indexGetStateNonLocking(anotherIndexDescriptor)
    ).thenReturn(org.neo4j.internal.kernel.api.InternalIndexState.ONLINE)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // given I run the call once
    cachingDecorator.indexGetStateNonLocking(indexDescriptor) shouldEqual expectedIndexState
    // then the delegate should have been called
    verify(mockDelegate).indexGetStateNonLocking(indexDescriptor)
    // when I run the call again
    cachingDecorator.indexGetStateNonLocking(indexDescriptor) shouldEqual expectedIndexState
    verifyNoMoreInteractions(mockDelegate)

    // when I use another index descriptor
    cachingDecorator.indexGetStateNonLocking(anotherIndexDescriptor)
    verify(mockDelegate).indexGetStateNonLocking(anotherIndexDescriptor)
  }

  test("should not cache locking index state ") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val expectedIndexState = org.neo4j.internal.kernel.api.InternalIndexState.ONLINE
    when(mockDelegate.indexGetState(indexDescriptor)).thenReturn(expectedIndexState)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // given I run the call once
    cachingDecorator.indexGetState(indexDescriptor) shouldEqual expectedIndexState
    // then the delegate should have been called
    verify(mockDelegate).indexGetState(indexDescriptor)
    // when I run the call again
    cachingDecorator.indexGetState(indexDescriptor) shouldEqual expectedIndexState
    // then the delegate should have been called again
    verify(mockDelegate, times(2)).indexGetState(indexDescriptor)
  }

  // New tests covering indexSize and indexUniqueValuesSelectivity caching behavior
  test("indexSize should use cached index sample") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val sample = new IndexSample(500L, 50, 5)
    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(sample)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // first call should delegate
    cachingDecorator.indexSize(indexDescriptor) shouldEqual 500L
    verify(mockDelegate).indexSample(indexDescriptor)

    // subsequent calls should use cache
    cachingDecorator.indexSize(indexDescriptor) shouldEqual 500L
    verifyNoMoreInteractions(mockDelegate)
  }

  test("indexUniqueValuesSelectivity should use cached index sample when sample non-zero") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val sample = new IndexSample(100L, 10, 100L)
    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(sample)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // compute selectivity = uniqueValues / sampleSize = 10 / 100 = 0.1
    cachingDecorator.indexUniqueValuesSelectivity(indexDescriptor) shouldEqual 0.1d
    verify(mockDelegate).indexSample(indexDescriptor)

    // subsequent calls should use cache
    cachingDecorator.indexUniqueValuesSelectivity(indexDescriptor) shouldEqual 0.1d
    verifyNoMoreInteractions(mockDelegate)
  }

  test("indexUniqueValuesSelectivity should return 1.0 when sample size is zero and use cache") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val sample = new IndexSample(0L, 0, 0)
    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(sample)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    cachingDecorator.indexUniqueValuesSelectivity(indexDescriptor) shouldEqual 1.0d
    verify(mockDelegate).indexSample(indexDescriptor)

    // subsequent calls should use cache
    cachingDecorator.indexUniqueValuesSelectivity(indexDescriptor) shouldEqual 1.0d
    verifyNoMoreInteractions(mockDelegate)
  }

  test("multiple methods share the same cached sample per descriptor and distinct descriptors are independent") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val anotherIndexDescriptor = mock[IndexDescriptor]

    val sample = new IndexSample(200L, 20, 200L)
    val sample2 = new IndexSample(300L, 30, 300L)

    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(sample)
    when(mockDelegate.indexSample(anotherIndexDescriptor)).thenReturn(sample2)

    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // call different methods for the same descriptor
    cachingDecorator.indexUniqueValuesSelectivity(indexDescriptor) shouldEqual 20.0 / 200.0
    cachingDecorator.indexSize(indexDescriptor) shouldEqual 200L
    cachingDecorator.indexSample(indexDescriptor) shouldEqual sample

    // delegate should have been called only once for this descriptor
    verify(mockDelegate).indexSample(indexDescriptor)

    // calls for another descriptor should trigger delegate separately
    cachingDecorator.indexSize(anotherIndexDescriptor) shouldEqual 300L
    verify(mockDelegate).indexSample(anotherIndexDescriptor)
  }

  // Additional tests to ensure cache stability even if delegate would change return values
  test("cached sample is stable even if delegate would return a different sample later") {
    val mockDelegate = mock[SchemaRead]
    val indexDescriptor = mock[IndexDescriptor]
    val firstSample = new IndexSample(100L, 10, 1)
    val secondSample = new IndexSample(200L, 20, 2)

    when(mockDelegate.indexSample(indexDescriptor)).thenReturn(firstSample, secondSample)
    val cachingDecorator = CachingSchemaReadDecorator(mockDelegate)

    // first call delegates and caches firstSample
    cachingDecorator.indexSample(indexDescriptor) shouldEqual firstSample
    verify(mockDelegate).indexSample(indexDescriptor)

    // change on delegate should not affect cached value
    cachingDecorator.indexSample(indexDescriptor) shouldEqual firstSample
    verifyNoMoreInteractions(mockDelegate)
  }
}
