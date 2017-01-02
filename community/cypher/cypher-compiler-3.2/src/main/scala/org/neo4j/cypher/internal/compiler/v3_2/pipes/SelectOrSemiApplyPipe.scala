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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id

case class SelectOrSemiApplyPipe(source: Pipe, inner: Pipe, predicate: Predicate, negated: Boolean)
                                (val id: Id = new Id)
                                (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {
  def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.filter {
      (outerContext) =>
        predicate.isTrue(outerContext)(state) || {
          val innerState = state.withInitialContext(outerContext)
          val innerResults = inner.createResults(innerState)
          if (negated) innerResults.isEmpty else innerResults.nonEmpty
        }
    }
  }

  private def name = if (negated) "SelectOrAntiSemiApply" else "SelectOrSemiApply"
}
