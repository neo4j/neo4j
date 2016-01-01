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
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.{PlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.graphdb.Node

import scala.collection.mutable

case class NodeHashJoinPipe(nodeIdentifier: String, left: Pipe, right: Pipe)
                           (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(left, pipeMonitor) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val table = new mutable.HashMap[Long, mutable.MutableList[ExecutionContext]]
    input.foreach { context =>
      context(nodeIdentifier) match {
        case n: Node =>
          val joinKey = n.getId
          val seq = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
          seq += context

        case null =>
      }
    }

    right.createResults(state).flatMap { context =>
      context(nodeIdentifier) match {
        case n: Node =>
          val joinKey = n.getId
          val seq = table.getOrElse(joinKey, mutable.MutableList.empty)
          seq.map(context ++ _)

        case null =>
          Iterator.empty
      }
    }
  }

  def planDescription: PlanDescription =
    new PlanDescriptionImpl(
      pipe = this,
      name = "NodeHashJoin",
      children = TwoChildren(left.planDescription, right.planDescription),
      arguments = Seq(KeyNames(Seq(nodeIdentifier)))
    )

  def symbols: SymbolTable = left.symbols.add(right.symbols.identifiers).add(nodeIdentifier, CTNode)

  override val sources = Seq(left, right)

  def dup(sources: List[Pipe]): Pipe = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)
  }

  override def localEffects = Effects.NONE
}
