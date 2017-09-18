/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.values.storable.Values

case class OptionalPipe(nullableVariables: Set[String], source: Pipe)
                       (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(source) {

  private def notFoundExecutionContext(initialContext: Option[ExecutionContext]): ExecutionContext = {
    val context = initialContext.getOrElse(ExecutionContext.empty)
    nullableVariables.foreach(v => context += v -> Values.NO_VALUE)
    context
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    if (input.isEmpty) Iterator(notFoundExecutionContext(state.initialContext))
    else input
}
