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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v3_4.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.internal.kernel.api._

class NodeIndexSeekOperator(longsPerRow: Int, refsPerRow: Int, offset: Int,
                            label: LabelToken,
                            propertyKey: PropertyKeyToken,
                            valueExpr: Expression) extends Operator {

  private var reference: IndexReference = CapableIndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == CapableIndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyKey.nameId.id)
    }
    reference
  }

  override def operate(message: Message,
                       data: Morsel,
                       context: QueryContext,
                       state: QueryState): Continuation = {
    var nodeCursor: NodeValueIndexCursor  = null
    var iterationState: Iteration = null
    val read = context.transactionalContext.dataRead
    val currentRow = new MorselExecutionContext(data, longsPerRow, refsPerRow, currentRow = 0)
    val queryState = new OldQueryState(context, resources = null, params = state.params)

    message match {
      case StartLeafLoop(is) =>
        nodeCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
        read.nodeIndexSeek(reference(context), nodeCursor, IndexOrder.NONE,
                           IndexQuery.exact(propertyKey.nameId.id, valueExpr(currentRow, queryState) ))
        iterationState = is
      case ContinueLoopWith(ContinueWithSource(it, is, _)) =>
        nodeCursor = it.asInstanceOf[NodeValueIndexCursor]
        iterationState = is
      case _ => throw new IllegalStateException()

    }

    val longs: Array[Long] = data.longs

    var processedRows = 0
    var hasMore = true
    while (processedRows < data.validRows && hasMore) {
      hasMore = nodeCursor.next()
      if (hasMore) {
        longs(processedRows * longsPerRow + offset) = nodeCursor.nodeReference()
        processedRows += 1
      }
    }

    data.validRows = processedRows

    if (hasMore)
      ContinueWithSource(nodeCursor, iterationState, needsSameThread = false)
    else {
      if (nodeCursor != null) {
        nodeCursor.close()
        nodeCursor = null
      }
      EndOfLoop(iterationState)
    }
  }

  override def addDependency(pipeline: Pipeline): Dependency = NoDependencies
}