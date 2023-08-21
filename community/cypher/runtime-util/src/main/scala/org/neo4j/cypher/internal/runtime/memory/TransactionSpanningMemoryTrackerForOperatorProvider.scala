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
package org.neo4j.cypher.internal.runtime.memory

import org.neo4j.memory.HeapMemoryTracker

/**
 * Gives the ability to track heap memory per operator, spanning multiple transactions.
 * Also allows to inspect the maximum memory per operator.
 */
trait TransactionSpanningMemoryTrackerForOperatorProvider {

  /**
   * Get the high water mark of allocated heap memory of this operator, in bytes.
   *
   * @return the maximum number of allocated memory bytes, or [[org.neo4j.memory.HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED]], if memory tracking was not enabled.
   */
  def heapHighWaterMarkOfOperator(operatorId: Int): Long

  /**
   * Get the memory tracker for the operator with the given id.
   * This memory tracker is not bound to any transaction.
   *
   * The reason that this is package private is to prevent runtime to call this method.
   * They should instead call [[MemoryTrackerForOperatorProvider.memoryTrackerForOperator()]],
   * which returns a MemoryTracker that is also bound to the current transaction.
   *
   * @param operatorId the id of the operator
   */
  private[memory] def memoryTrackerForOperator(operatorId: Int): HeapMemoryTracker
}
