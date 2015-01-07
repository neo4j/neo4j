/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0.{ExecutionContext, PlanDescription}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.Expression
import org.neo4j.cypher.internal.helpers.CollectionSupport


class UnwindPipe(source: Pipe, collection: Expression, identifier: String) extends PipeWithSource(source) with CollectionSupport {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.flatMap {
      context =>
        val seq = makeTraversable(collection(context)(state))
        seq.map(x => context.newWith((identifier, x)))
    }

  def executionPlanDescription: PlanDescription =
    source.executionPlanDescription.andThen(this, "UNWIND")

  def symbols = source.symbols.add(identifier, collection.getType(source.symbols).legacyIteratedType)

  override def readsFromDatabase = false
}

