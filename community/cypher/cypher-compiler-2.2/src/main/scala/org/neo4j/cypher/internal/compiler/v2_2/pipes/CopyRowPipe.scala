/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects

final case class CopyRowPipe(source: Pipe)(val estimatedCardinality: Option[Long] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new Iterator[ExecutionContext] {
      def hasNext: Boolean = input.hasNext
      def next(): ExecutionContext = input.next().clone()
    }

  def planDescription = source.planDescription.andThen(this, "CopyRow")

  val symbols = source.symbols

  override def localEffects = Effects.NONE

  def setEstimatedCardinality(estimated: Long) = copy()(Some(estimated))

  override def requiredRowLifetime: RowLifetime = ChainedLifetime
  override def providedRowLifetime: RowLifetime = QueryLifetime
}
