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
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{IndexSeek, IndexSeekMode, NodeIndexSeeker, QueryState => OldQueryState}
import org.neo4j.cypher.internal.runtime.slotted.pipes.SlottedIndexedProperty
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.{IndexedNodeWithProperties, QueryContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.QueryExpression
import org.neo4j.internal.kernel.api._
import org.opencypher.v9_0.expressions.LabelToken

class NodeIndexSeekOperator(offset: Int,
                            label: LabelToken,
                            properties: Array[SlottedIndexedProperty],
                            argumentSize: SlotConfiguration.Size,
                            override val valueExpr: QueryExpression[Expression],
                            override val indexMode: IndexSeekMode = IndexSeek)
  extends StreamingOperator with NodeIndexSeeker {

  private val propertyIndicesWithValues: Array[Int] = properties.zipWithIndex.filter(_._1.getValueFromIndex).map(_._2)
  val propertyOffsets: Array[Int] = properties.map(_.slotOffset).collect{ case Some(o) => o }

  override def init(context: QueryContext, state: QueryState, currentRow: MorselExecutionContext): ContinuableOperatorTask = {
    val queryState = new OldQueryState(context, resources = null, params = state.params)
    val indexReference = reference(context)
    val tupleIterator = indexSeek(queryState, indexReference, propertyIndicesWithValues, currentRow)
    new OTask(tupleIterator)
  }

  override val propertyIds: Array[Int] = properties.map(_.propertyKeyId)

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyIds:_*)
    }
    reference
  }

  class OTask(tupleIterator: Iterator[IndexedNodeWithProperties]) extends ContinuableOperatorTask {
    override def operate(currentRow: MorselExecutionContext,
                         context: QueryContext,
                         state: QueryState): Unit = {

      var processedRows = 0
      while (currentRow.hasMoreRows && tupleIterator.hasNext) {
        val IndexedNodeWithProperties(node, values) = tupleIterator.next()
        currentRow.setLongAt(offset, node.id())
        propertyOffsets.foreach {
          offset => currentRow.setRefAt(offset, values(offset))
        }
        currentRow.moveToNextRow()
      }

      currentRow.finishedWriting()
    }

    override def canContinue: Boolean = tupleIterator.hasNext
  }
}
