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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue

/**
 * Abstraction for grouping expressions used for aggregation and distinct.
 */
trait GroupingExpression {
  type KeyType <: AnyValue

  /**
   * Computes the grouping key, it will either be a single AnyValue or a SequenceValue of AnyValues
   * @param context used for evaluating expressions
   * @param state used for evaluating expressions
   * @return the grouping key
   */
  def computeGroupingKey(context: ReadableRow, state: QueryState): KeyType

  /**
   * Compute the grouping key for all columns that have a provided order.
   *
   * @param groupingKey the whole grouping key, as obtained by [[computeGroupingKey()]]
   * @return the grouping key
   */
  def computeOrderedGroupingKey(groupingKey: KeyType): AnyValue

  /**
   * Retrieves an already computed and projected key. Can be called after a call to project grouping key.
   * @param context The context to get the values for the key
   * @return The grouping key as read from the context
   */
  def getGroupingKey(context: CypherRow): KeyType

  /**
   * Projects a computed key to the context
   * @param context The context where to project.
   * @param groupingKey The computed grouping key to project
   */
  def project(context: WritableRow, groupingKey: KeyType): Unit

  /**
   * @return `true` if there is no grouping, `false` otherwise.
   */
  def isEmpty: Boolean
}
