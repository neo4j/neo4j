/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression, NumericHelper}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId

case class LimitPipe(source: Pipe, exp: Expression)
                    (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) with NumericHelper {

  exp.registerOwningPipe(this)

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    if(input.isEmpty)
      return Iterator.empty

    val limit = asInt(exp(state.createOrGetInitialContext(executionContextFactory), state))

    input.take(limit.value())
  }
}
