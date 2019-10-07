/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.util.{LabelId, PropertyKeyId, Selectivity}
import org.neo4j.internal.kernel.api.{Read, SchemaRead}

class TransactionBoundGraphStatisticsTest extends CypherFunSuite {

  private val labelId = 42
  private val propertyId = 1337
  private val index = IndexDescriptor(LabelId(labelId), Seq(PropertyKeyId(propertyId)))

  test("indexPropertyExistsSelectivity should compute selectivity") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(500L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity(0.5)))
  }

  test("indexPropertyExistsSelectivity should handle indexSize being out-of-sync with counts") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(1000L)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("indexPropertyExistsSelectivity should handle label count zero") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(read.countsForNodeWithoutTxState(labelId)).thenReturn(0L)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.indexPropertyExistsSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should compute selectivity") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(schemaRead.indexUniqueValuesSelectivity(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(0.5)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity(0.001)))
  }

  test("uniqueValueSelectivity should handle selectivity greater than 1") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(schemaRead.indexUniqueValuesSelectivity(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(0.0001)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(100)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ONE))
  }

  test("uniqueValueSelectivity should handle indexSize zero") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(schemaRead.indexUniqueValuesSelectivity(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(0.5)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(0L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

  test("uniqueValueSelectivity should handle indexUniqueValuesSelectivity zero") {
    //given
    val read = mock[Read]
    val schemaRead = mock[SchemaRead]
    when(schemaRead.indexUniqueValuesSelectivity(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(0.0)
    when(schemaRead.indexSize(schemaRead.indexReferenceUnchecked(labelId, propertyId))).thenReturn(2000L)

    //when
    val statistics = TransactionBoundGraphStatistics(read, schemaRead)

    //then
    statistics.uniqueValueSelectivity(index) should equal(Some(Selectivity.ZERO))
  }

}
