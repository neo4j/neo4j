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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DistinctPipe.GroupingCol
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection.DistinctSet
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

/**
 * Specialization of [[DistinctPipe]] that leverages the order of some grouping columns.
 */
case class OrderedDistinctPipe(source: Pipe, groupingColumns: Array[GroupingCol])(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  private val (orderedKeyNames, unorderedKeyNames) = {
    val (ordered, unordered) = groupingColumns.partition(_.ordered)
    (ordered.map(_.key), unordered.map(_.key))
  }

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    /*
     * The filtering is done by extracting from the context the values of all return expressions, and keeping them
     * in a set.
     */
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    var seen: DistinctSet[AnyValue] = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
    state.query.resources.trace(seen)
    var currentOrderedGroupingValue: AnyValue = null

    input.filter { ctx =>
      var i = 0
      while (i < groupingColumns.length) {
        ctx.set(groupingColumns(i).key, groupingColumns(i).expression(ctx, state))
        i += 1
      }

      val orderedGroupingValues = buildValueList(ctx, orderedKeyNames)
      val unorderedGroupingValues = buildValueList(ctx, unorderedKeyNames)

      if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != orderedGroupingValues) {
        currentOrderedGroupingValue = orderedGroupingValues
        seen.close()
        seen = DistinctSet.createDistinctSet[AnyValue](memoryTracker)
        state.query.resources.trace(seen)
      }
      val added = seen.add(unorderedGroupingValues)
      added
    }.closing(seen)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case OrderedDistinctPipe(otherSource, otherGroupingColumns) =>
        otherSource == this.source && otherGroupingColumns.sameElements(this.groupingColumns)
      case _ => false
    }
  }

  private def buildValueList(ctx: CypherRow, keyNames: Array[String]): ListValue = {
    val builder = ListValueBuilder.newListBuilder(keyNames.length)
    keyNames.foreach(name => builder.add(ctx.getByName(name)))
    builder.build()
  }
}

/**
 * Specialization of [[OrderedDistinctPipe]] for the case that all groupingColumns are ordered.
 */
case class AllOrderedDistinctPipe(source: Pipe, groupingColumns: Array[GroupingCol])(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {

  // First the ordered columns, then the unordered ones
  private val keyNames = groupingColumns.map(_.key)

  protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    var currentOrderedGroupingValue: AnyValue = null

    input.filter { ctx =>
      var i = 0
      while (i < groupingColumns.length) {
        ctx.set(groupingColumns(i).key, groupingColumns(i).expression(ctx, state))
        i += 1
      }
      val groupingValue = VirtualValues.list(keyNames.map(ctx.getByName): _*)

      if (currentOrderedGroupingValue == null || currentOrderedGroupingValue != groupingValue) {
        currentOrderedGroupingValue = groupingValue
        true
      } else {
        false
      }
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case AllOrderedDistinctPipe(otherSource, otherGroupingColumns) =>
        otherSource == this.source && otherGroupingColumns.sameElements(this.groupingColumns)
      case _ => false
    }
  }
}
