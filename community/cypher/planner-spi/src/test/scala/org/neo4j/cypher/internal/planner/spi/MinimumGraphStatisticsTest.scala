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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_INDEX_PROPERTY_EXISTS_SELECTIVITY
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics.MIN_NODES_WITH_LABEL
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MinimumGraphStatisticsTest extends CypherFunSuite {

  test("should return the actual isNotNull selectivity when node count is above minimum") {
    // Given
    val labelId = LabelId(0)
    val indexDescriptor = IndexDescriptor.forLabel(IndexDescriptor.IndexType.Range, labelId, Seq(PropertyKeyId(1)))
    val delegate = mock[GraphStatistics]
    when(delegate.nodesWithLabelCardinality(Some(labelId))).thenReturn(Cardinality(MIN_NODES_WITH_LABEL + 1))
    val isNotNullSelectivity = Selectivity(0.4)
    when(delegate.indexPropertyIsNotNullSelectivity(indexDescriptor)).thenReturn(Some(isNotNullSelectivity))
    val stats = new MinimumGraphStatistics(delegate)

    // When
    val maybeSelectivity = stats.indexPropertyIsNotNullSelectivity(indexDescriptor)

    // Then
    maybeSelectivity shouldBe Some(isNotNullSelectivity)
  }

  test("should return a fixed isNotNull selectivity when node count is below minimum") {
    // Given
    val labelId = LabelId(0)
    val indexDescriptor = IndexDescriptor.forLabel(IndexDescriptor.IndexType.Range, labelId, Seq(PropertyKeyId(1)))
    val delegate = mock[GraphStatistics]
    when(delegate.nodesWithLabelCardinality(Some(labelId))).thenReturn(Cardinality.SINGLE)
    val isNotNullSelectivity = Selectivity(0.4)
    when(delegate.indexPropertyIsNotNullSelectivity(indexDescriptor)).thenReturn(Some(isNotNullSelectivity))
    val stats = new MinimumGraphStatistics(delegate)

    // When
    val maybeSelectivity = stats.indexPropertyIsNotNullSelectivity(indexDescriptor)

    // Then
    maybeSelectivity shouldBe Some(MIN_INDEX_PROPERTY_EXISTS_SELECTIVITY)
  }

  test("should not decrease the isNotNull selectivity below MIN_INDEX_PROPERTY_EXISTS_SELECTIVITY") {
    // Given
    val labelId = LabelId(0)
    val indexDescriptor = IndexDescriptor.forLabel(IndexDescriptor.IndexType.Range, labelId, Seq(PropertyKeyId(1)))
    val delegate = mock[GraphStatistics]
    when(delegate.nodesWithLabelCardinality(Some(labelId))).thenReturn(Cardinality(1))
    val isNotNullSelectivity = Selectivity(0.04)
    when(delegate.indexPropertyIsNotNullSelectivity(indexDescriptor)).thenReturn(Some(isNotNullSelectivity))
    val stats = new MinimumGraphStatistics(delegate)

    // When
    val maybeSelectivity = stats.indexPropertyIsNotNullSelectivity(indexDescriptor)

    // Then
    maybeSelectivity shouldBe Some(MinimumGraphStatistics.MIN_INDEX_PROPERTY_EXISTS_SELECTIVITY)
  }

  test("should not return selectivity if the underlying delegate does not return a selectivity") {
    // Given
    val labelId = LabelId(0)
    val indexDescriptor = IndexDescriptor.forLabel(IndexDescriptor.IndexType.Range, labelId, Seq(PropertyKeyId(1)))
    val delegate = mock[GraphStatistics]
    when(delegate.nodesWithLabelCardinality(Some(labelId))).thenReturn(Cardinality(1))
    when(delegate.indexPropertyIsNotNullSelectivity(indexDescriptor)).thenReturn(None)
    val stats = new MinimumGraphStatistics(delegate)

    // When
    val maybeSelectivity = stats.indexPropertyIsNotNullSelectivity(indexDescriptor)

    // Then
    maybeSelectivity shouldBe None
  }

  test("should return values from delegate if cardinality is above threshold") {
    for (isNotNullSelectivity <- Seq(Some(Selectivity(0.4)), Some(Selectivity(0.04)), None)) {
      // Given
      val labelId = LabelId(0)
      val indexDescriptor = IndexDescriptor.forLabel(IndexDescriptor.IndexType.Range, labelId, Seq(PropertyKeyId(1)))
      val delegate = mock[GraphStatistics]
      when(delegate.nodesWithLabelCardinality(Some(labelId))).thenReturn(Cardinality(11))
      when(delegate.indexPropertyIsNotNullSelectivity(indexDescriptor)).thenReturn(isNotNullSelectivity)
      val stats = new MinimumGraphStatistics(delegate)

      // When
      val maybeSelectivity = stats.indexPropertyIsNotNullSelectivity(indexDescriptor)

      // Then
      maybeSelectivity shouldBe isNotNullSelectivity
    }
  }
}
