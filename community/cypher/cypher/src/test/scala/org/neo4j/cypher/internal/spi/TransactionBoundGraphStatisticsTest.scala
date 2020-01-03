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
package org.neo4j.cypher.internal.spi

import java.util.Collections.singletonList

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId, Selectivity}
import org.neo4j.internal.kernel.api.{Read, SchemaRead}
import org.neo4j.internal.schema
import org.neo4j.internal.schema.{IndexPrototype, SchemaDescriptor}

class TransactionBoundGraphStatisticsTest extends CypherFunSuite {

  private val labelId = 42
  private val propertyId = 1337
  private val index = IndexDescriptor(LabelId(labelId), Seq(PropertyKeyId(propertyId)))
  private val descriptor: schema.IndexDescriptor = IndexPrototype.forSchema(SchemaDescriptor.forLabel(labelId, propertyId)).withName("wut!").materialise(11L)
  private var read: Read = _
  private var schemaRead: SchemaRead = _


  test("indexPropertyExistsSelectivity should compute selectivity") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(descriptor)).thenReturn(500L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity(0.5)))
  }

  test("indexPropertyExistsSelectivity should handle indexSize being out-of-sync with counts") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("indexPropertyExistsSelectivity should handle label count zero") {
    //given
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(0L)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should compute selectivity") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.5)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity(0.001)))
  }

  test("uniqueValueSelectivity should handle selectivity greater than 1") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0001)
    when(schemaRead.indexSize(descriptor)).thenReturn(100)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("uniqueValueSelectivity should handle indexSize zero") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.5)
    when(schemaRead.indexSize(descriptor)).thenReturn(0L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should handle indexUniqueValuesSelectivity zero") {
    //given
    when(schemaRead.indexUniqueValuesSelectivity(descriptor)).thenReturn(0.0)
    when(schemaRead.indexSize(descriptor)).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  override protected def beforeEach(): Unit = {
    read = mock[Read]
    schemaRead = mock[SchemaRead]
    when(schemaRead.index(any[SchemaDescriptor])).thenReturn(singletonList(descriptor).iterator())
  }
}
