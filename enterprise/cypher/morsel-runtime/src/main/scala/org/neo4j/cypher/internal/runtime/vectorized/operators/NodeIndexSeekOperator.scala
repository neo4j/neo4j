/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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