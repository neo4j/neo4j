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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.util.v3_4.attribution.Id

import scala.collection.JavaConverters._

case class ForeachPipe(source: Pipe, inner: Pipe, variable: String, expression: Expression)
                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) with ListSupport {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map {
      (outerContext) =>
        val values = makeTraversable(expression(outerContext, state))
        values.iterator().asScala.foreach { v =>
          val innerState = state.withInitialContext(executionContextFactory.copyWith(outerContext, variable, v))
          inner.createResults(innerState).length // exhaust the iterator, in case there's a merge read increasing cardinality inside the foreach
        }
        outerContext
    }
}
