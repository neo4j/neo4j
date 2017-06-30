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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.values.virtual.VirtualValues

case class RollUpApplyPipe(lhs: Pipe, rhs: Pipe, collectionName: String, identifierToCollect: String, nullableIdentifiers: Set[String])
                          (val id: Id = new Id)
  extends PipeWithSource(lhs) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map {
      ctx =>
        if (nullableIdentifiers.map(ctx).contains(null)) {
          ctx += collectionName -> null
        } else {
          val original = ctx.createClone()
          val innerState = state.withInitialContext(ctx)
          val innerResults = rhs.createResults(innerState)
          val collection = VirtualValues.list(innerResults.map(m => m(identifierToCollect)).toArray:_*)
          original += collectionName -> collection
        }
    }
  }
}
