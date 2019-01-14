/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.SlotConfiguration
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes._
import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_4.logical.plans.QueryExpression
import org.neo4j.internal.kernel.api.{CapableIndexReference, IndexReference}

case class NodeIndexSeekSlottedPipe(ident: String,
                                    label: LabelToken,
                                    propertyKeys: Seq[PropertyKeyToken],
                                    valueExpr: QueryExpression[Expression],
                                    indexMode: IndexSeekMode = IndexSeek,
                                    slots: SlotConfiguration,
                                    argumentSize: SlotConfiguration.Size)
                                   (val id: Id = Id.INVALID_ID) extends Pipe with NodeIndexSeeker {

  private val offset = slots.getLongOffsetFor(ident)

  override val propertyIds: Array[Int] = propertyKeys.map(_.nameId.id).toArray

  private var reference: IndexReference = CapableIndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == CapableIndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyIds:_*)
    }
    reference
  }

  valueExpr.expressions.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val indexReference = reference(state.query)
    val baseContext = state.createOrGetInitialContext(executionContextFactory)
    val resultNodes = indexSeek(state, indexReference, baseContext)
    resultNodes.map { node =>
      val context = SlottedExecutionContext(slots)
      state.copyArgumentStateTo(context, argumentSize.nLongs, argumentSize.nReferences)
      context.setLongAt(offset, node.id)
      context
    }
  }

}
