/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.{InternalPlanDescription, PlanDescriptionImpl, TwoChildren}
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.graphdb.Node

import scala.collection.mutable

case class NodeHashJoinPipe(nodeIdentifiers: Set[String], left: Pipe, right: Pipe)
                           (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(left, pipeMonitor) with RonjaPipe {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      return Iterator.empty

    val rhsIterator = right.createResults(state)

    if (rhsIterator.isEmpty)
      return Iterator.empty

    val table = buildProbeTable(input)

    if (table.isEmpty)
      return Iterator.empty

    val result = for {context: ExecutionContext <- rhsIterator
                      joinKey <- computeKey(context)}
    yield {
      val seq = table.getOrElse(joinKey, mutable.MutableList.empty)
      seq.map(context ++ _)
    }

    result.flatten
  }

  def planDescriptionWithoutCardinality: InternalPlanDescription =
    new PlanDescriptionImpl(
      id = id,
      name = "NodeHashJoin",
      children = TwoChildren(left.planDescription, right.planDescription),
      arguments = Seq(KeyNames(nodeIdentifiers.toSeq)),
      identifiers
    )

  def symbols = left.symbols.add(right.symbols.identifiers)

  override val sources = Seq(left, right)

  def dup(sources: List[Pipe]): Pipe = {
    val (left :: right :: Nil) = sources
    copy(left = left, right = right)(estimatedCardinality)
  }

  override def localEffects = Effects()

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  private def buildProbeTable(input: Iterator[ExecutionContext]): mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]] = {
    val table = new mutable.HashMap[Vector[Long], mutable.MutableList[ExecutionContext]]

    for {context <- input
         joinKey <- computeKey(context)} {
      val seq = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      seq += context
    }

    table
  }

  private val cachedIdentifiers = nodeIdentifiers.toIndexedSeq

  private def computeKey(context: ExecutionContext): Option[Vector[Long]] = {
    val key = new Array[Long](cachedIdentifiers.length)

    for (idx <- 0 until cachedIdentifiers.length) {
      key(idx) = context(cachedIdentifiers(idx)) match {
        case n: Node => n.getId
        case null => return None
        case _ => throw new CypherTypeException("Created a plan that uses non-nodes when expecting a node")
      }
    }
    Some(key.toVector)
  }
}
