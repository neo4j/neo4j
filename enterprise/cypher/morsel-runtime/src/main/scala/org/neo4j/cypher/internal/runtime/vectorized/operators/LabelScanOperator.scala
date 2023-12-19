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
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(longsPerRow: Int, refsPerRow: Int, offset: Int, label: LazyLabel) extends Operator {

  override def operate(message: Message,
                       data: Morsel,
                       context: QueryContext,
                       state: QueryState): Continuation = {
    var nodeCursor: NodeLabelIndexCursor  = null
    var iterationState: Iteration = null
    val read = context.transactionalContext.dataRead
    val labelId = label.getOptId(context)
    if (labelId.isEmpty) return EndOfLoop(iterationState)

    message match {
      case StartLeafLoop(is) =>
        nodeCursor = context.transactionalContext.cursors.allocateNodeLabelIndexCursor()
        read.nodeLabelScan(labelId.get.id,  nodeCursor)
        iterationState = is
      case ContinueLoopWith(ContinueWithSource(it, is, _)) =>
        nodeCursor = it.asInstanceOf[NodeLabelIndexCursor]
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