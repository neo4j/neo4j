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
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._

case class DirectedRelationshipByIdSeekPipe(ident: String, relIdExpr: SeekArgs, toNode: String, fromNode: String)
                                           (val id: Id = Id.INVALID_ID) extends Pipe {

  relIdExpr.registerOwningPipe(this)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val ctx = state.newExecutionContext(executionContextFactory)
<<<<<<< HEAD
    val relIds = VirtualValues.dropNoValues(relIdExpr.expressions(ctx, state))
=======
    val relIds = VirtualValues.filter(relIdExpr.expressions(ctx, state), new function.Predicate[AnyValue] {
      override def test(t: AnyValue): Boolean = t != Values.NO_VALUE
    })
>>>>>>> f5e80af3c9ccd2984f5be2bfad767e59e542dc1f
    new DirectedRelationshipIdSeekIterator(
      ident,
      fromNode,
      toNode,
      ctx,
      executionContextFactory,
      state.query.relationshipOps,
      relIds.iterator().asScala
    )
  }

 }
