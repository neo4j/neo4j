/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.indexQuery
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.QueryExpression
import org.neo4j.cypher.internal.frontend.v3_3.ast.LabelToken
import org.neo4j.cypher.internal.frontend.v3_3.ast.PropertyKeyToken
import org.neo4j.values.virtual.VirtualValues.fromNodeProxy

case class NodeIndexSeekPipe(ident: String,
                             label: LabelToken,
                             propertyKeys: Seq[PropertyKeyToken],
                             valueExpr: QueryExpression[Expression],
                             indexMode: IndexSeekMode = IndexSeek)(val id: Id = new Id)
    extends Pipe {

  private val propertyIds: Array[Int] = propertyKeys.map(_.nameId.id).toArray

  private val descriptor = IndexDescriptor(label.nameId.id, propertyIds)

  private val indexFactory = indexMode.indexFactory(descriptor)

  valueExpr.expressions.foreach(_.registerOwningPipe(this))

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val index = indexFactory(state)
    val baseContext = state.createOrGetInitialContext()
    val resultNodes = indexQuery(valueExpr, baseContext, state, index, label.name, propertyKeys.map(_.name))
    resultNodes.map(node => baseContext.newWith1(ident, fromNodeProxy(node)))
  }

}
