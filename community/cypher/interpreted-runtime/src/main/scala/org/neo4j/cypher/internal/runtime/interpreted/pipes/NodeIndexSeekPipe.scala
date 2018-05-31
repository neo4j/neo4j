/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.opencypher.v9_0.util.attribution.Id
import org.opencypher.v9_0.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.internal.kernel.api.IndexReference

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             propertyKeys: Seq[PropertyKeyToken],
                             valueExpr: QueryExpression[Expression],
                             indexMode: IndexSeekMode = IndexSeek)
                            (val id: Id = Id.INVALID_ID) extends Pipe with NodeIndexSeeker {

  override val propertyIds: Array[Int] = propertyKeys.map(_.nameId.id).toArray

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, propertyIds:_*)
    }
    reference
  }

  valueExpr.expressions.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val indexReference = reference(state.query)
    val baseContext = state.createOrGetInitialContext(executionContextFactory)
    val resultNodes = indexSeek(state, indexReference, baseContext)
    resultNodes.map(node => executionContextFactory.copyWith(baseContext, ident, node))
  }
}
