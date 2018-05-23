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
import org.neo4j.values.storable.NumberValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}

import scala.collection.JavaConverters._

/**
  * This sorts the first N rows of the morsel in place. If N > morselSize, this is equivalent to PreSortOperator
  */
class PreTopNOperator(orderBy: Seq[ColumnOrder], slots: SlotConfiguration, countExpression: Expression) extends MiddleOperator {

  // TODO?
  //  countExpression.registerOwningPipe(this)

  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {

    val comparator: Comparator[Object] = orderBy
      .map(MorselSorting.createComparator(data, slots))
      .reduce((a, b) => a.thenComparing(b))

    // create an array of indices that we can sort
    var arrayToSort: Array[Object] = MorselSorting.createArray(data)

    var readingPos = 0
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val firstRow = new MorselExecutionContext(data, longCount, refCount, currentRow = readingPos)
    val queryState = new OldQueryState(context, resources = null, params = state.params)
    val count = countExpression(firstRow, queryState).asInstanceOf[NumberValue].longValue().toInt


    if(count < data.validRows) {
      // a table to hold the top n entries
      val topTable = new DefaultComparatorTopTable(comparator, count)

      while (readingPos < data.validRows) {
        topTable.add(arrayToSort(readingPos))
        readingPos += 1
      }

      topTable.sort()

      arrayToSort = topTable.iterator.asScala.toArray

      // only the first count elements stay valid
      data.validRows = count
    } else {
      // We have to sort everything
      java.util.Arrays.sort(arrayToSort, comparator)
    }

    // convert back to morsel arrays
    val (newLongs, newRefs) = MorselSorting.createSortedMorselData(data, arrayToSort, slots)

    // Copy the now sorted arrays into the Morsel
    System.arraycopy(newLongs, 0, data.longs, 0, newLongs.length)
    System.arraycopy(newRefs, 0, data.refs, 0, newRefs.length)
  }

}
