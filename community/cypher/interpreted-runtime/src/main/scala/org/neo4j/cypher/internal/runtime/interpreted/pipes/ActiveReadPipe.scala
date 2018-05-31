/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.opencypher.v9_0.util.attribution.Id

case class ActiveReadPipe(source: Pipe)(val id: Id = Id.INVALID_ID) extends Pipe {

  override def createResults(state: QueryState): Iterator[ExecutionContext] = {
    val activeState = state.withQueryContext(state.query.withActiveRead)
    val sourceResult = source.createResults(activeState)

    state.decorator.decorate(this, state)
    state.decorator.decorate(this, sourceResult)
  }

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    throw new UnsupportedOperationException("This method should never be called on ActiveReadPipe")
}
