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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.graphdb.Node
import org.neo4j.cypher.internal.compiler.v2_1
import v2_1.{PlanDescriptionImpl, PlanDescription, ExecutionContext}
import v2_1.symbols._
import scala.collection.mutable

case class NodeOuterHashJoinPipe(node: String, source: Pipe, inner: Pipe, nullableIdentifiers: Set[String])
                      (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val probeTable = new mutable.HashMap[Long, mutable.MutableList[ExecutionContext]]
    input.foreach { context =>
      val joinKey = context(node).asInstanceOf[Node].getId
      val seq = probeTable.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      seq += context
    }

    val seenKeys = mutable.Set[Long]()
    val iter = inner.createResults(state).flatMap { context =>
      val joinKey = context(node).asInstanceOf[Node].getId
      seenKeys.add(joinKey)
      val seq = probeTable.getOrElse(joinKey, mutable.MutableList.empty)
      seq.map(context ++ _)
    }

    lazy val nullRows = (probeTable.keySet -- seenKeys).iterator.flatMap {
      x => probeTable(x).map {
        ctx => nullableIdentifiers.foldLeft(ctx) {
          case (acc, id) => acc += id -> null
        }
      }
    }

    iter ++ nullRows
  }

  def executionPlanDescription: PlanDescription =
    new PlanDescriptionImpl(
      this,
      "NodeOuterHashJoin",
      Seq(source.executionPlanDescription, inner.executionPlanDescription),
      args = Seq("node" -> node)
    )

  def symbols: SymbolTable = source.symbols.add(inner.symbols.identifiers).add(node, CTNode)
}
