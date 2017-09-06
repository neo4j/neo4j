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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.cypher.internal.javacompat.ValueUtils
import org.scalatest.mock.MockitoSugar

import scala.collection.Map

class FakeSlottedPipeFromVariables(val data: Iterator[Map[String, Any]],
                                   newVariables: (String, CypherType)*) extends Pipe with MockitoSugar {

  val pipeline: PipelineInformation = registerAllocation(newVariables)

  def this(data: Traversable[Map[String, Any]], variables: (String, CypherType)*) = {
    this(data.toIterator, variables: _*)
  }

  def registerAllocation(vars: Seq[(String, CypherType)]): PipelineInformation = {
    val pipeInfo = PipelineInformation.empty
    vars.foreach {
      case (name, typ) => pipeInfo.newReference(name, true, typ)
    }
    pipeInfo
  }

  def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    data.map(m => {
      val newContext = PrimitiveExecutionContext(pipeline)
      m.mapValues(ValueUtils.of).foreach {
        case (k, v) =>
          newContext.setRefAt(pipeline.getReferenceOffsetFor(k), v)
      }
      newContext
    })

  var id = new Id
}
