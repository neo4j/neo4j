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
package org.neo4j.cypher.internal.spi

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.schema
import org.neo4j.internal.schema.IndexPrototype
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.logging.Log

import java.util.Collections
import java.util.Collections.singletonList

class TransactionBoundGraphStatisticsTest extends CypherFunSuite {

  private val labelId = 42
  private val propertyId = 1337
  private val index = IndexDescriptor.forLabel(LabelId(labelId), Seq(PropertyKeyId(propertyId)))
  private val descriptor: schema.IndexDescriptor = IndexPrototype.forSchema(SchemaDescriptor.forLabel(labelId, propertyId)).withName("wut!").materialise(11L)
  private var read: Read = _
  private var schemaRead: SchemaRead = _
  private val log = mock[Log]


  test("indexPropertyExistsSelectivity should compute selectivity") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(descriptor)).thenReturn(500L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.indexPropertyIsNotNullSelectivity(index) should equal(Some(Selectivity(0.5)))
  }

  test("indexPropertyExistsSelectivity should handle indexSize being out-of-sync with counts") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.indexPropertyIsNotNullSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("indexPropertyExistsSelectivity should handle label count zero") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(0L)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.indexPropertyIsNotNullSelectivity(index) should equal(MinimumGraphStatistics.MIN_INDEX_PROPERTY_EXISTS_SELECTIVITY)
  }

  test("indexPropertyExistsSelectivity should log if index returned from schema read but size cannot get computed") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(20L)
    val exception = new IndexNotFoundKernelException("wut")
    when(schemaRead.indexSize(any[org.neo4j.internal.schema.IndexDescriptor])).thenThrow(exception)
    val theLog = mock[Log]

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, theLog)

    //then
    statistics.indexPropertyIsNotNullSelectivity(index) should equal(None)
    verify(theLog).debug("Index not found for indexPropertyExistsSelectivity", exception)
  }

  test("indexPropertyExistsSelectivity should not log if index was not found") {
    when(schemaRead.index(any[SchemaDescriptor])).thenReturn(Collections.emptyIterator[org.neo4j.internal.schema.IndexDescriptor]())
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(20L)
    val exception = new IndexNotFoundKernelException("wut")
    when(schemaRead.indexSize(any[org.neo4j.internal.schema.IndexDescriptor])).thenThrow(exception)
    val theLog = mock[Log]

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, theLog)

    //then
    statistics.indexPropertyIsNotNullSelectivity(index) should equal(None)
    verifyNoInteractions(theLog)
  }

  test("uniqueValueSelectivity should compute selectivity") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.5)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity(0.001)))
  }

  test("uniqueValueSelectivity should handle selectivity greater than 1") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0001)
    when(schemaRead.indexSize(descriptor)).thenReturn(100)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("uniqueValueSelectivity should handle indexSize zero") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.5)
    when(schemaRead.indexSize(descriptor)).thenReturn(0L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should handle indexUniqueValuesSelectivity zero") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, log)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should log if index returned from schema read but size cannot get computed") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0)
    val exception = new IndexNotFoundKernelException("wut")
    when(schemaRead.indexSize(any[org.neo4j.internal.schema.IndexDescriptor])).thenThrow(exception)
    val theLog = mock[Log]

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, theLog)

    //then
    statistics.uniqueValueSelectivity(index) should equal(None)
    verify(theLog).debug("Index not found for uniqueValueSelectivity", exception)
  }

  test("uniqueValueSelectivity should not log if index was not found") {
    //given
    when(schemaRead.index(any[SchemaDescriptor])).thenReturn(Collections.emptyIterator[org.neo4j.internal.schema.IndexDescriptor]())
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0)
    val exception = new IndexNotFoundKernelException("wut")
    when(schemaRead.indexSize(any[org.neo4j.internal.schema.IndexDescriptor])).thenThrow(exception)
    val theLog = mock[Log]

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead, theLog)

    //then
    statistics.uniqueValueSelectivity(index) should equal(None)
    verifyNoInteractions(theLog)
  }

  override protected def beforeEach(): Unit = {
    read = mock[Read]
    schemaRead = mock[SchemaRead]
    when(schemaRead.index(any[SchemaDescriptor])).thenReturn(singletonList(descriptor).iterator())
  }
}
