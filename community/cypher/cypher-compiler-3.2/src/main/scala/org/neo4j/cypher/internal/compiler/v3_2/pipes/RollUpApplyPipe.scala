/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_2.symbols.ListType

case class RollUpApplyPipe(lhs: Pipe, rhs: Pipe, collectionName: String, identifierToCollect: String, nullableIdentifiers: Set[String])
                          (val id: Id = new Id)
                          (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(lhs, pipeMonitor) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        if (nullableIdentifiers.map(ctx).contains(null)) {
          ctx += collectionName -> null
        } else {
          val original = ctx.clone()
          val innerState = state.withInitialContext(ctx)
          val innerResults = rhs.createResults(innerState)
          val collection = innerResults.map(m => m(identifierToCollect)).toList
          original += collectionName -> collection
        }
    }
  }

  override def symbols =
    lhs.symbols.add(collectionName, ListType(rhs.symbols.variables(identifierToCollect)))

  override def dup(sources: List[Pipe]) = {
    val (l :: r :: Nil) = sources
    RollUpApplyPipe(l, r, collectionName, identifierToCollect, nullableIdentifiers)(id)
  }
}
