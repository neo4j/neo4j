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
package org.neo4j.cypher.internal.compiler.v3_1.pipes

import org.neo4j.cypher.internal.compiler.v3_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription.Arguments.KeyNames

case class ProduceResultsPipe(source: Pipe, columns: Seq[String])(val estimatedCardinality: Option[Double] = None)
                             (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats

    input.map {
      original =>
        val m = MutableMaps.create(columns.size)
        columns.foreach {
          case (name) => m.put(name, original(name))
        }

        ExecutionContext(m)
    }
  }

  def planDescriptionWithoutCardinality = source.planDescription
    .andThen(this.id, "ProduceResults", variables, KeyNames(columns))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  def localEffects = Effects()

  def symbols = source.symbols.filter(columns.contains)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }
}
