/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized.operators

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(longsPerRow: Int, refsPerRow: Int, offset: Int, label: LazyLabel)
  extends NodeIndexOperator[NodeLabelIndexCursor](longsPerRow, refsPerRow, offset) {

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
      case ContinueLoopWith(ContinueWithSource(cursor, is)) =>
        nodeCursor = cursor.asInstanceOf[NodeLabelIndexCursor]
        iterationState = is
      case _ => throw new IllegalStateException()

    }

    iterate(data, nodeCursor, iterationState)
  }

  override def addDependency(pipeline: Pipeline): Dependency = NoDependencies
}
