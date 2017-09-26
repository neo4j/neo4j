/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState =>  OldQueryState}
/*
Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class FilterOperator(pipeline: PipelineInformation, predicate: Predicate) extends MiddleOperator {
  override def operate(iterationState: Iteration,
                       data: Morsel,
                       context: QueryContext,
                       state: QueryState): Unit = {

    var readingPos = 0
    var writingPos = 0
    val longCount = pipeline.numberOfLongs
    val refCount = pipeline.numberOfReferences
    val currentRow = new MorselExecutionContext(data, longCount, refCount, currentRow = readingPos)
    val longs = data.longs
    val objects = data.refs
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while (readingPos < data.validRows) {
      currentRow.currentRow = readingPos
      if (predicate.isTrue(currentRow, queryState)) {
        System.arraycopy(data.longs, readingPos * longCount, longs, longCount * writingPos, longCount)
        System.arraycopy(data.refs, readingPos * refCount, objects, refCount * writingPos, refCount)
        writingPos += 1
      }
      readingPos += 1
      currentRow.currentRow = readingPos
    }

    data.validRows = writingPos
  }
}
