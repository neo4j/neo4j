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

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor

class LabelScanOperator(offset: Int, label: LazyLabel, argumentSize: SlotConfiguration.Size)
  extends NodeIndexOperator[NodeLabelIndexCursor](offset) {

  override def init(context: QueryContext, state: QueryState, inputMorsel: MorselExecutionContext): ContinuableOperatorTask = {
    val cursor = context.transactionalContext.cursors.allocateNodeLabelIndexCursor()
    val read = context.transactionalContext.dataRead
    val labelId = label.getOptId(context)
    read.nodeLabelScan(labelId.get.id, cursor)
    new OTask(cursor)
  }

  class OTask(nodeCursor: NodeLabelIndexCursor) extends ContinuableOperatorTask {

    var hasMore = false
    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {
      hasMore = iterate(currentRow, nodeCursor, argumentSize)
    }

    override def canContinue: Boolean = hasMore
  }
}
