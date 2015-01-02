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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{InternalPlanDescription, NoChildren, PlanDescriptionImpl}
import org.neo4j.cypher.internal.compiler.v2_2.symbols.{SymbolTable, SymbolTypeAssertionCompiler, _}

case class ArgumentPipe(symbols: SymbolTable)
                       (val estimatedCardinality: Option[Long] = None)
                       (implicit val monitor: PipeMonitor) extends Pipe with RonjaPipe {
  def sources = Seq.empty

  def withEstimatedCardinality(estimated: Long): Pipe with RonjaPipe = copy()(Some(estimated))

  def planDescription: InternalPlanDescription =
    new PlanDescriptionImpl(this, "Argument", NoChildren, Seq.empty, identifiers)

  private val typeAssertions =
    SymbolTypeAssertionCompiler.compile(
      symbols.identifiers.toSeq.collect { case entry@(_, typ) if typ == CTNode || typ == CTRelationship => entry}
    )

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    Iterator(typeAssertions(state.initialContext.get))


  def dup(sources: List[Pipe]): Pipe = this

  def exists(pred: (Pipe) => Boolean) = pred(this)
}
