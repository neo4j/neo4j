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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{LongSlot, RefSlot, SlotConfiguration}
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.symbols
import org.neo4j.cypher.result.QueryResult
import org.neo4j.values.AnyValue


class ProduceResultOperator(slots: SlotConfiguration, fieldNames: Array[String]) extends MiddleOperator {
  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {
    val resultRow = new MorselResultRow(data, 0, slots, fieldNames, context)
    (0 until data.validRows) foreach { position =>
      resultRow.currentPos = position
      state.visitor.visit(resultRow)
    }
  }
}

class MorselResultRow(var morsel: Morsel,
                      var currentPos: Int,
                      slots: SlotConfiguration,
                      fieldNames: Array[String],
                      queryContext: QueryContext) extends QueryResult.Record {
  private val array = new Array[AnyValue](fieldNames.length)

  private val updateArray: Array[() => AnyValue] = fieldNames.map(key => slots.get(key) match {
    case None => throw new IllegalStateException()
    case Some(RefSlot(offset, _, _)) => () =>
       morsel.refs(currentPos * slots.numberOfReferences + offset)
    case Some(LongSlot(offset, _, symbols.CTNode)) => () =>
      queryContext.nodeOps.getById(morsel.longs(currentPos * slots.numberOfLongs + offset))
    case Some(LongSlot(offset, _, symbols.CTRelationship)) => () =>
      queryContext.relationshipOps.getById(morsel.longs(currentPos * slots.numberOfLongs + offset))
    case _ => throw new IllegalStateException
  })

  override def fields(): Array[AnyValue] = {
    var i = 0
    while ( i < array.length) {
      array(i) = updateArray(i)()
      i += 1
    }
    array
  }
}
