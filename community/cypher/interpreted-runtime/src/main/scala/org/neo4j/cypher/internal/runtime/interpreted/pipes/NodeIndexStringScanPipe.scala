/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.v3_5.logical.plans.{CachedNodeProperty, IndexOrder, IndexedProperty}
import org.neo4j.internal.kernel.api.{IndexReference, NodeValueIndexCursor}
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.cypher.internal.v3_5.expressions.LabelToken
import org.neo4j.cypher.internal.v3_5.util.CypherTypeException
import org.neo4j.cypher.internal.v3_5.util.attribution.Id

abstract class AbstractNodeIndexStringScanPipe(ident: String,
                                               label: LabelToken,
                                               property: IndexedProperty,
                                               valueExpr: Expression) extends Pipe with IndexPipeWithValues {

  override val indexPropertyIndices: Array[Int] = if (property.shouldGetValue) Array(0) else Array.empty
  override val indexCachedNodeProperties: Array[CachedNodeProperty] = Array(property.asCachedNodeProperty(ident))
  protected val needsValues = indexPropertyIndices.nonEmpty

  private var reference: IndexReference = IndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == IndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id, property.propertyKeyToken.nameId.id)
    }
    reference
  }

  valueExpr.registerOwningPipe(this)

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.newExecutionContext(executionContextFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
        new IndexIterator(state.query, baseContext, queryContextCall(state, reference(state.query), value))
      case Values.NO_VALUE =>
        Iterator.empty
      case x => throw new CypherTypeException(s"Expected a string value, but got $x")
    }

    resultNodes
  }

  protected def queryContextCall(state: QueryState,
                                 indexReference: IndexReference,
                                 value: TextValue): NodeValueIndexCursor
}

case class NodeIndexContainsScanPipe(ident: String,
                                     label: LabelToken,
                                     property: IndexedProperty,
                                     valueExpr: Expression,
                                     indexOrder: IndexOrder)
                                    (val id: Id = Id.INVALID_ID)
  extends AbstractNodeIndexStringScanPipe(ident, label, property, valueExpr) {

  override protected def queryContextCall(state: QueryState,
                                          indexReference: IndexReference,
                                          value: TextValue): NodeValueIndexCursor =
    state.query.indexSeekByContains(indexReference, needsValues, indexOrder, value)
}

case class NodeIndexEndsWithScanPipe(ident: String,
                                     label: LabelToken,
                                     property: IndexedProperty,
                                     valueExpr: Expression,
                                     indexOrder: IndexOrder)
                                    (val id: Id = Id.INVALID_ID)
  extends AbstractNodeIndexStringScanPipe(ident, label, property, valueExpr) {

  override protected def queryContextCall(state: QueryState,
                                          indexReference: IndexReference,
                                          value: TextValue): NodeValueIndexCursor =
    state.query.indexSeekByEndsWith(indexReference, needsValues, indexOrder, value)
}
