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

import org.neo4j.cypher.{CypherTypeException, InternalException}
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.PlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{PlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.compiler.v2_2.symbols._
import org.neo4j.graphdb.Node

import scala.collection.mutable

case class NodeHashJoinPipe(nodeIdentifiers: Set[String], left: Pipe, right: Pipe)
                           (val estimatedCardinality: Option[Long] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(left, pipeMonitor) with RonjaPipe {

  val identifiers = nodeIdentifiers.toIndexedSeq

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {

    val table = buildProbeTable(input)

    val result = for {context: ExecutionContext <- right.createResults(state)
                      joinKey <- computeKey(context)}
    yield {
      val seq = table.getOrElse(joinKey, mutable.MutableList.empty)
      seq.map(context ++ _)
    }

    result.flatten
  }

  def planDescription: PlanDescription =
    new PlanDescriptionImpl(
      pipe = this,
      name = "NodeHashJoin",
      children = TwoChildren(left.planDescription, right.planDescription),
      _arguments = Seq(KeyNames(nodeIdentifiers.toSeq))
    )

  def symbols: SymbolTable = left.symbols.add(right.symbols.identifiers)

  override val sources = Seq(left, right)

  def dup(sources: List[Pipe]): Pipe = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)(estimatedCardinality)
  }

  override def localEffects = Effects.NONE

  def withEstimatedCardinality(estimated: Long) = copy()(Some(estimated))

  private def buildProbeTable(input: Iterator[ExecutionContext]): mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]] = {
    val table = new mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]]

    for {context <- input
         joinKey <- computeKey(context)} {
      val seq = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      seq += context
    }

    table
  }

  private def computeKey(context: ExecutionContext): Option[Vector[Long]] = {
    val key = new Array[Long](identifiers.length)

    for (idx <- 0 until identifiers.length) {
      key(idx) = context(identifiers(idx)) match {
        case n: Node => n.getId
        case null => return None
        case _ => throw new CypherTypeException("Created a plan that uses non-nodes when expecting a node")
      }
    }
    Some(key.toVector)
  }

}
