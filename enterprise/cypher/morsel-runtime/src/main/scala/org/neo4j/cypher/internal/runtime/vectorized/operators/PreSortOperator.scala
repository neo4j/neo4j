/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.slotted.DefaultComparatorTopTable
import org.neo4j.cypher.internal.runtime.slotted.pipes.ColumnOrder
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.values.storable.NumberValue

import scala.collection.JavaConverters._

/*
 * Responsible for sorting the Morsel in place, which will then be merged together with other sorted Morsels
 * If countExpression != None, this sorts the first N rows of the morsel in place. If N > morselSize, this is equivalent to sorting everything.
 */
class PreSortOperator(orderBy: Seq[ColumnOrder], slots: SlotConfiguration, countExpression: Option[Expression] = None) extends MiddleOperator {

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {

    val comparator: Comparator[Integer] = orderBy
      .map(MorselSorting.compareMorselIndexesByColumnOrder(data, slots))
      .reduce((a, b) => a.thenComparing(b))

    // First we create an array of the same size as the rows in the morsel that we'll sort.
    // This array contains only the pointers to the morsel rows
    var arrayToSort: Array[Integer] = MorselSorting.createMorselIndexesArray(data)

    // potentially calculate the limit
    val maybeLimit = countExpression.map { count =>
      val firstRow = new MorselExecutionContext(data, slots.numberOfLongs, slots.numberOfReferences, currentRow = 0)
      val queryState = new OldQueryState(context, resources = null, params = state.params)
      count(firstRow, queryState).asInstanceOf[NumberValue].longValue().toInt
    }

    maybeLimit match {
      case Some(limit) if limit < data.validRows =>
        // a table to hold the top n entries
        val topTable = new DefaultComparatorTopTable(comparator, limit)

        var readingPos = 0
        while (readingPos < data.validRows) {
          topTable.add(arrayToSort(readingPos))
          readingPos += 1
        }

        topTable.sort()

        arrayToSort = topTable.iterator.asScala.toArray

        // only the first count elements stay valid
        data.validRows = limit
      case _ =>
        // We have to sort everything
        java.util.Arrays.sort(arrayToSort, comparator)
    }
    // Now that we have a sorted array, we need to shuffle the morsel rows around until they follow the same order
    // as the sorted array
    // TODO: Do this without creating extra arrays
    val (newLongs, newRefs) = MorselSorting.createSortedMorselData(data, arrayToSort, slots)

    // Copy the now sorted arrays into the Morsel
    System.arraycopy(newLongs, 0, data.longs, 0, newLongs.length)
    System.arraycopy(newRefs, 0, data.refs, 0, newRefs.length)
  }
}
