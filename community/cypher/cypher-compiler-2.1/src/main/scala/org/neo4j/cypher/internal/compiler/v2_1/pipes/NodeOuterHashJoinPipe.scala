/**
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{PlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.Node

import scala.collection.mutable

case class NodeOuterHashJoinPipe(node: String, source: Pipe, inner: Pipe, nullableIdentifiers: Set[String])
                                (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  val nullColumns: Map[String, Any] = nullableIdentifiers.map(_ -> null).toMap

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val probeTable = new mutable.HashMap[Long, mutable.MutableList[ExecutionContext]]
    val nullLhsRows = input.flatMap { context =>
      context(node) match {
        case null =>
          Some(context)

        case node:Node =>
          val joinKey = node.getId
          val seq = probeTable.getOrElseUpdate(joinKey, mutable.MutableList.empty)
          seq += context
          None
      }
    }

    val seenKeys = mutable.Set[Long]()
    val joinedRows = inner.createResults(state).flatMap { context =>
      context(node) match {
        case n:Node =>
          val joinKey = n.getId
          seenKeys.add(joinKey)
          val seq = probeTable.getOrElse(joinKey, mutable.MutableList.empty)
          seq.map(context ++ _)

        case _ =>
          None
      }
    }

    lazy val rowsWithoutRhsMatch: Iterator[ExecutionContext] = (probeTable.keySet -- seenKeys).iterator.flatMap {
      x => probeTable(x).map(addNulls)
    }
    val rowsWithNullAsJoinKey: Iterator[ExecutionContext] = nullLhsRows.map(addNulls)
    rowsWithNullAsJoinKey ++ joinedRows ++ rowsWithoutRhsMatch
  }

  private def addNulls(in:ExecutionContext): ExecutionContext = in.newWith(nullColumns)


  def planDescription: PlanDescription =
    new PlanDescriptionImpl(this,
      "NodeOuterHashJoin",
      TwoChildren(source.planDescription, inner.planDescription),
      Seq.empty
    )

  def symbols: SymbolTable = source.symbols.add(inner.symbols.identifiers).add(node, CTNode)

  override val sources = Seq(source, inner)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: inner :: Nil) = sources
    copy(source = source, inner = inner)
  }

  override def localEffects = Effects.NONE
}
