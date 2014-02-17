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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.compiler.v2_1.spi.{DelegatingOperations, Operations, QueryContext, DelegatingQueryContext}
import org.neo4j.graphdb.{PropertyContainer, Relationship, Node}

class PeriodicCommitQueryContext(batchSize: Long, inner: QueryContext) extends DelegatingQueryContext(inner) {
  var currentCount: Long = 0
  var commitCount: Long = 0

  override def removeLabelsFromNode(node: Long, labelIds: Iterator[Int]): Int =
    countWhenNeeded(super.removeLabelsFromNode(node, labelIds))

  override def createRelationship(start: Node, end: Node, relType: String): Relationship =
    incrementWhenNeeded(super.createRelationship(start, end, relType))

  override def createNode(): Node = incrementWhenNeeded(super.createNode())

  override def setLabelsOnNode(node: Long, labelIds: Iterator[Int]): Int =
    countWhenNeeded(super.setLabelsOnNode(node, labelIds))

  override val nodeOps: Operations[Node] = new TxResettingOps(super.nodeOps)

  override val relationshipOps: Operations[Relationship] = new TxResettingOps(super.relationshipOps)

  class TxResettingOps[T <: PropertyContainer](inner: Operations[T]) extends DelegatingOperations[T](inner) {

    override def removeProperty(obj: Long, propertyKeyId: Int): Unit =
      incrementWhenNeeded(super.removeProperty(obj, propertyKeyId))

    override def setProperty(obj: Long, propertyKey: Int, value: Any): Unit =
      incrementWhenNeeded(super.setProperty(obj, propertyKey, value))

    override def delete(obj: T): Unit = incrementWhenNeeded(super.delete(obj))
  }

  private def incrementWhenNeeded[T](f: => T): T = {
    val result = f
    resetWhenNeeded(1)
    result
  }

  private def countWhenNeeded(f: => Int): Int = {
    val result = f
    if (result > 0)
      resetWhenNeeded(result)
    result
  }

  private def resetWhenNeeded(increment: Int) {
    // !!! DO NOT CALL WITH increment < 1

    currentCount += increment

    if (currentCount % batchSize == 0)
      commitAndRestartTx()
  }

  override def close(success: Boolean) {
    super.close(success)
    if (success) {
        commitCount += 1
    }
  }

  override def commitAndRestartTx() {
    super.commitAndRestartTx()
    commitCount += 1
  }
}

