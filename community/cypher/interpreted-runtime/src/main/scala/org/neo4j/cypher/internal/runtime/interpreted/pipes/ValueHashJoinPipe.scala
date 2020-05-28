/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.Iterators
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.kernel.impl.util.collection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.asScalaIteratorConverter

case class ValueHashJoinPipe(lhsExpression: Expression, rhsExpression: Expression, left: Pipe, right: Pipe)
                            (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(left) {

  override protected def internalCreateResults(input: Iterator[CypherRow], state: QueryState): Iterator[CypherRow] = {

    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input, state)

    if (table.isEmpty) {
      table.close()
      return Iterator.empty
    }

    val result = for {rhsRow <- rhsIterator
                      joinKey = rhsExpression(rhsRow, state) if !(joinKey eq Values.NO_VALUE) }
      yield {
        val lhsRows = table.get(joinKey)
        lhsRows.asScala.map { lhsRow =>
          val outputRow = lhsRow.createClone()
          outputRow.mergeWith(rhsRow, state.query)
          outputRow
        }
      }

    Iterators.resourceClosingIterator(result.flatten, table)
  }

  private def buildProbeTable(input: Iterator[CypherRow], state: QueryState): collection.ProbeTable[AnyValue, CypherRow] = {
    val table = collection.ProbeTable.createProbeTable[AnyValue, CypherRow](state.memoryTracker.memoryTrackerForOperator(id.x))

    for (context <- input;
         joinKey = lhsExpression(context, state) if joinKey != null) {
      table.put(joinKey, context)
    }

    table
  }
}
