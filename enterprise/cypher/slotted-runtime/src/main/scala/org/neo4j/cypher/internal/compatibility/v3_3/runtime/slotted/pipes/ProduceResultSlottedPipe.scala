/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.kernel.impl.util.{NodeProxyWrappingNodeValue, RelationshipProxyWrappingEdgeValue}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{ListValue, MapValue}

case class ProduceResultSlottedPipe(source: Pipe, columns: Seq[(String, Expression)])
                                   (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) with Pipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    // do not register this pipe as parent as it does not do anything except filtering of already fetched
    // key-value pairs and thus should not have any stats

    input.map {
      original =>
        val m = MutableMaps.create(columns.size)
        columns.foreach {
          case (name, exp) => {
            val value = exp(original, state)
            if (state.prePopulateResult) {
              populate(value)
            }
            m.put(name, value)
          }
        }
        ExecutionContext(m)
    }
  }

  def populate(value: AnyValue): Unit = {
    value match {
      case n: NodeProxyWrappingNodeValue =>
        n.populate()
      case r: RelationshipProxyWrappingEdgeValue =>
        r.populate()
      case l: ListValue =>
        val it = l.iterator()
        while (it.hasNext) {
          populate(it.next())
        }
      case m: MapValue =>
        val it = m.entrySet().iterator()
        while (it.hasNext) {
          populate(it.next().getValue)
        }
    }
  }
}
