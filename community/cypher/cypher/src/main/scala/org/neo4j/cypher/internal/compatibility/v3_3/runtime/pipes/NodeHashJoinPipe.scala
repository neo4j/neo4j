/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.cypher.internal.frontend.v3_3.CypherTypeException
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

import scala.collection.mutable

case class NodeHashJoinPipe(nodeVariables: Set[String], left: Pipe, right: Pipe)
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends PipeWithSource(left) {

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
      seq.map(context.mergeWith)
    }

    result.flatten
  }

  private def buildProbeTable(input: Iterator[ExecutionContext]): mutable.HashMap[IndexedSeq[Long], mutable.MutableList[ExecutionContext]] = {
    val table = new mutable.HashMap[IndexedSeq[Long], mutable.MutableList[ExecutionContext]]

    for {context <- input
         joinKey <- computeKey(context)} {
      val seq = table.getOrElseUpdate(joinKey, mutable.MutableList.empty)
      seq += context
    }

    table
  }

  private val cachedVariables = nodeVariables.toIndexedSeq

  private def computeKey(context: ExecutionContext): Option[IndexedSeq[Long]] = {
    val key = new Array[Long](cachedVariables.length)

    for (idx <- cachedVariables.indices) {
      key(idx) = context(cachedVariables(idx)) match {
        case n: NodeValue => n.id()
        case Values.NO_VALUE => return None
        case _ => throw new CypherTypeException("Created a plan that uses non-nodes when expecting a node")
      }
    }
    Some(key.toIndexedSeq)
  }
}
