/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.util.v3_4.attribution.Id
import org.neo4j.cypher.internal.v3_4.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.internal.kernel.api.{CapableIndexReference, IndexReference}
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.values.virtual.NodeValue

abstract class AbstractNodeIndexStringScanPipe(ident: String,
                                               label: LabelToken,
                                               propertyKey: PropertyKeyToken,
                                               valueExpr: Expression) extends Pipe {


  private var reference: IndexReference = CapableIndexReference.NO_INDEX

  private def reference(context: QueryContext): IndexReference = {
    if (reference == CapableIndexReference.NO_INDEX) {
      reference = context.indexReference(label.nameId.id,  propertyKey.nameId.id)
    }
    reference
  }

  valueExpr.registerOwningPipe(this)

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.createOrGetInitialContext(executionContextFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
        queryContextCall(state, reference(state.query), value.stringValue()).
          map(node => executionContextFactory.copyWith(baseContext, ident, node))
      case Values.NO_VALUE =>
        Iterator.empty
      case x => throw new CypherTypeException(s"Expected a string value, but got $x")
    }

    resultNodes
  }

  protected def queryContextCall(state: QueryState, indexReference: IndexReference, value: String): Iterator[NodeValue]

}

case class NodeIndexContainsScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val id: Id = Id.INVALID_ID)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override protected def queryContextCall(state: QueryState, indexReference: IndexReference, value: String) =
    state.query.indexScanByContains(indexReference, value)
}

case class NodeIndexEndsWithScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val id: Id = Id.INVALID_ID)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override protected def queryContextCall(state: QueryState, indexReference: IndexReference, value: String) =
    state.query.indexScanByEndsWith(indexReference, value)
}
