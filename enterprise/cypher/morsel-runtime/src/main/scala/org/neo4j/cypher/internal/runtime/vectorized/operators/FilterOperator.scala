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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
/*
Takes an input morsel and compacts all rows to the beginning of it, only keeping the rows that match a predicate
 */
class FilterOperator(predicate: Predicate) extends MiddleOperator {
  override def operate(iterationState: Iteration,
                       readingRow: MorselExecutionContext,
                       context: QueryContext,
                       state: QueryState): Unit = {

    val writingRow = readingRow.createClone()
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    while (readingRow.hasMoreRows) {
      val matches = predicate.isTrue(readingRow, queryState)
      if (matches) {
        writingRow.copyFrom(readingRow)
        writingRow.moveToNextRow()
      }
      readingRow.moveToNextRow()
    }

    writingRow.finishedWriting()
  }
}
