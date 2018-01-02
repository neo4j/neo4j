/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
/*
Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class FilterOperator(slots: SlotConfiguration, predicate: Predicate) extends MiddleOperator {
  override def operate(iterationState: Iteration,
                       data: Morsel,
                       context: QueryContext,
                       state: QueryState): Unit = {

    var readingPos = 0
    var writingPos = 0
    val longCount = slots.numberOfLongs
    val refCount = slots.numberOfReferences
    val currentRow = new MorselExecutionContext(data, longCount, refCount, currentRow = readingPos)
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while (readingPos < data.validRows) {
      currentRow.currentRow = readingPos
      val matches = predicate.isTrue(currentRow, queryState)
      if (matches) {
        System.arraycopy(data.longs, readingPos * longCount, data.longs, writingPos * longCount, longCount)
        System.arraycopy(data.refs, readingPos * refCount, data.refs, writingPos * refCount, refCount)
        writingPos += 1
      }
      readingPos += 1
      currentRow.currentRow = readingPos
    }

    data.validRows = writingPos
  }
}
