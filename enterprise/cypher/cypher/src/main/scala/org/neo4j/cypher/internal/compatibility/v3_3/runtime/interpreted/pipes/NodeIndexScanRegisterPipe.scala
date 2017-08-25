/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.PrimitiveLongHelper
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.IndexDescriptor
import org.neo4j.cypher.internal.frontend.v3_3.ast.{LabelToken, PropertyKeyToken}


case class NodeIndexScanRegisterPipe(ident: String,
                                     label: LabelToken,
                                     propertyKey: PropertyKeyToken,
                                     pipelineInformation: PipelineInformation)
                                    (val id: Id = new Id)
  extends Pipe {

  private val offset = pipelineInformation.getLongOffsetFor(ident)

  private val descriptor = IndexDescriptor(label.nameId.id, propertyKey.nameId.id)

  protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val nodes = state.query.indexScanPrimitive(descriptor)
    PrimitiveLongHelper.map(nodes, { node =>
      val context = PrimitiveExecutionContext(pipelineInformation)
      state.copyArgumentStateTo(context, pipelineInformation.numberOfLongs, pipelineInformation.numberOfReferences)
      context.setLongAt(offset, node)
      context
    })
  }

}
