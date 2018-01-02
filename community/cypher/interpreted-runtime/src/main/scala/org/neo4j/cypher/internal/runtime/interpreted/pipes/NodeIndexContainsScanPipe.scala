/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.internal.planner.v3_4.spi.IndexDescriptor
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.v3_4.expressions.{LabelToken, PropertyKeyToken}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.values.virtual.NodeValue

abstract class AbstractNodeIndexStringScanPipe(ident: String,
                                               label: LabelToken,
                                               propertyKey: PropertyKeyToken,
                                               valueExpr: Expression) extends Pipe {

  private val descriptor = IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  valueExpr.registerOwningPipe(this)

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val baseContext = state.createOrGetInitialContext(executionContextFactory)
    val value = valueExpr(baseContext, state)

    val resultNodes = value match {
      case value: TextValue =>
        queryContextCall(state, descriptor, value.stringValue()).
          map(node => baseContext.newWith1(ident, node))
      case Values.NO_VALUE =>
        Iterator.empty
      case x => throw new CypherTypeException(s"Expected a string value, but got $x")
    }

    resultNodes
  }

  protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String): Iterator[NodeValue]

}

case class NodeIndexContainsScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String) =
    state.query.indexScanByContains(indexDescriptor, value)
}

case class NodeIndexEndsWithScanPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     valueExpr: Expression)
                                    (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends AbstractNodeIndexStringScanPipe(ident, label, propertyKey, valueExpr) {

  override protected def queryContextCall(state: QueryState, indexDescriptor: IndexDescriptor, value: String) =
    state.query.indexScanByEndsWith(indexDescriptor, value)
}
