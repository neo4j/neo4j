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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.Slot
import org.neo4j.cypher.internal.runtime.slotted.helpers.SlottedPipeBuilderUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.runtime.interpreted.{ExecutionContext, ListSupport}
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

import scala.collection.JavaConverters._

case class ForeachSlottedPipe(lhs: Pipe, rhs: Pipe, innerVariableSlot: Slot, expression: Expression)
                             (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(lhs) with Pipe with ListSupport {

  //===========================================================================
  // Compile-time initializations
  //===========================================================================
  private val setVariableFun = makeSetValueInSlotFunctionFor(innerVariableSlot)

  //===========================================================================
  // Runtime code
  //===========================================================================
  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.map {
      (outerContext) =>
        val values = makeTraversable(expression(outerContext, state))
        values.iterator().asScala.foreach { v =>
          setVariableFun(outerContext, v) // A slot for the variable has been allocated on the outer context
          val innerState = state.withInitialContext(outerContext)
          rhs.createResults(innerState).length // exhaust the iterator, in case there's a merge read increasing cardinality inside the foreach
        }
        outerContext
    }
  }
}
