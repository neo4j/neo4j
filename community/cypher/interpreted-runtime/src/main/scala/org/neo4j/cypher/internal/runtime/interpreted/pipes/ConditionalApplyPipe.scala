/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.storable.Values

case class ConditionalApplyPipe(source: Pipe, inner: Pipe, items: Seq[String], negated: Boolean)
                               (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      (outerContext) =>
        if (condition(outerContext)) {
          val original = outerContext.createClone()
          val innerState = state.withInitialContext(outerContext)
          val innerResults = inner.createResults(innerState)
          innerResults.map { context => original mergeWith context }
        } else Iterator.single(outerContext)
    }

  private def condition(context: ExecutionContext) = {
    val cond = items.exists { context.get(_).get != Values.NO_VALUE}
      if (negated) !cond else cond
  }

  private def name = if (negated) "AntiConditionalApply" else "ConditionalApply"
}
