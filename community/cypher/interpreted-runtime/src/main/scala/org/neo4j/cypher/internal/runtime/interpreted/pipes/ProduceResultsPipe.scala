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

import org.neo4j.cypher.internal.runtime.{ExecutionContext, MutableMaps, ValuePopulation}
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.values.AnyValue

case class ProduceResultsPipe(source: Pipe, columns: Seq[String])
                             (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats

    if (state.prePopulateResults)
      input.map {
        original =>
          produceAndPopulate(original)
      }
    else
      input.map {
        original =>
          produce(original)
      }
  }

  private def produceAndPopulate(original: ExecutionContext) = {
    val m = MutableMaps.create[String, AnyValue](columns.size)
    columns.foreach(
      name => {
        val value = original.getByName(name)
        ValuePopulation.populate(value)
        m.put(name, value)
      }
    )
    ExecutionContext(m)
  }

  private def produce(original: ExecutionContext) = {
    val m = MutableMaps.create[String, AnyValue](columns.size)
    columns.foreach(
      name => m.put(name, original.getByName(name))
    )
    ExecutionContext(m)
  }
}
