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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.internal.kernel.api.helpers.StubRead
import org.neo4j.internal.kernel.api.helpers.StubSchemaRead
import org.neo4j.internal.schema
import org.neo4j.internal.schema.IndexDescriptor

class TransactionBoundIndexComparatorFactoryTest extends CommunityCypherTestSuite {

  private def givenIndexDescriptor(indexId: Long, name: String): schema.IndexDescriptor = {
    schema.IndexPrototype
      .uniqueForSchema(schema.SchemaDescriptors.forLabel(123, 321))
      .withName(name)
      .materialise(indexId)
  }

  test("should rank smaller indexes higher, given the constant unique values selectivity") {
    val brokenIndex = givenIndexDescriptor(0, "broken")
    val smallIndex = givenIndexDescriptor(1, "small")
    val mediumIndex = givenIndexDescriptor(2, "medium")
    val largeIndex = givenIndexDescriptor(3, "large")

    val read = new StubRead {
      override def estimateCountsForNode(labelId: Int): Long = 1000
    }
    val schemaRead = new StubSchemaRead {
      override def indexSize(index: schema.IndexDescriptor): Long = {
        index match {
          case `brokenIndex` => 2000
          case `smallIndex`  => 100
          case `mediumIndex` => 400
          case `largeIndex`  => 700
          case _             => throw new IllegalArgumentException(s"Unknown index $index")
        }
      }

      override def indexUniqueValuesSelectivity(index: IndexDescriptor): Double = {
        val uniqueValuesInIndex = 10 // same for all indexes
        uniqueValuesInIndex.toDouble / indexSize(index)
      }
    }

    val cmp = TransactionBoundIndexComparatorFactory.createComparator(read, schemaRead)

    val indexes = Seq(brokenIndex, smallIndex, mediumIndex, largeIndex)
    val expectedSorted = Seq(brokenIndex, largeIndex, mediumIndex, smallIndex)

    for (perm <- indexes.permutations) {
      perm.sorted(Ordering.comparatorToOrdering(cmp)) shouldEqual expectedSorted
    }
  }

  test("should rank indexes with more distinct values higher, given the constant index size") {
    val sameValues = givenIndexDescriptor(1, "same-values")
    val fewDistinctValues = givenIndexDescriptor(2, "few-distinct-values")
    val manyDistinctValues = givenIndexDescriptor(3, "many-distinct-values")

    val read = new StubRead {
      override def estimateCountsForNode(labelId: Int): Long = 1000
    }
    val schemaRead = new StubSchemaRead {
      override def indexSize(index: schema.IndexDescriptor): Long = {
        500
      }

      override def indexUniqueValuesSelectivity(index: IndexDescriptor): Double = {
        val uniqueValuesInIndex = index match {
          case `sameValues`         => 1
          case `fewDistinctValues`  => 10
          case `manyDistinctValues` => 100
          case _                    => throw new IllegalArgumentException(s"Unknown index $index")
        }
        uniqueValuesInIndex.toDouble / indexSize(index)
      }
    }

    val cmp = TransactionBoundIndexComparatorFactory.createComparator(read, schemaRead)

    val indexes = Seq(sameValues, fewDistinctValues, manyDistinctValues)
    val expectedSorted = Seq(sameValues, fewDistinctValues, manyDistinctValues)

    for (perm <- indexes.permutations) {
      perm.sorted(Ordering.comparatorToOrdering(cmp)) shouldEqual expectedSorted
    }
  }
}
