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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexSeek, IndexSeekMode, NodeIndexSeeker, QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.v3_5.logical.plans.QueryExpression
import org.neo4j.internal.kernel.api._
import org.neo4j.values.virtual.NodeValue
import org.opencypher.v9_0.expressions.{LabelToken, PropertyKeyToken}

class NodeIndexSeekOperator(offset: Int,
                            label: LabelToken,
                            propertyKeys: Seq[PropertyKeyToken],
                            argumentSize: SlotConfiguration.Size,
                            override val valueExpr: QueryExpression[Expression],
                            override val indexMode: IndexSeekMode = IndexSeek)
  extends Operator with NodeIndexSeeker {

  override def init(context: QueryContext, state: QueryState, currentRow: MorselExecutionContext): ContinuableOperatorTask = {
    val valueIndexCursor = context.transactionalContext.cursors.allocateNodeValueIndexCursor()
    val read = context.transactionalContext.dataRead
    val queryState = new OldQueryState(context, resources = null, params = state.params)
    val indexReference = reference(context)
    val nodeIterator = indexSeek(queryState, indexReference, currentRow)
    new OTask(nodeIterator)
  }

  override val propertyIds: Array[Int] = propertyKeys.map(_.nameId.id).toArray

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyIds:_*)
    }
    reference
  }

  class OTask(nodeIterator: Iterator[NodeValue]) extends ContinuableOperatorTask {
    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      var processedRows = 0
      while (currentRow.hasMoreRows && nodeIterator.hasNext) {
//        iterationState.copyArgumentStateTo(currentRow, argumentSize.nLongs, argumentSize.nReferences)
        currentRow.setLongAt(offset, nodeIterator.next().id())
        currentRow.moveToNextRow()
      }

      currentRow.finishedWriting()
    }

    override def canContinue: Boolean = nodeIterator.hasNext
  }
}
