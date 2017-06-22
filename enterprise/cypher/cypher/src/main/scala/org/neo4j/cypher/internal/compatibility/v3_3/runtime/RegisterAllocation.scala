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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.ir.v3_3.IdName

import scala.collection.mutable

object RegisterAllocation {
  def allocateRegisters(lp: LogicalPlan): Map[LogicalPlan, PipelineInformation] = {

    val result = new mutable.OpenHashMap[LogicalPlan, PipelineInformation]()

    def allocate(lp: LogicalPlan, pipelineInfo: PipelineInformation): Unit = lp match {
      case leaf: NodeLogicalLeafPlan =>
        pipelineInfo.newLong(leaf.idName.name)
        result += (lp -> pipelineInfo)

      case ProduceResult(_, source) =>
        allocate(source, pipelineInfo)
        result += (lp -> pipelineInfo)

      case Selection(_, source) =>
        allocate(source, pipelineInfo)
        result += (lp -> pipelineInfo)

      case Expand(source, _, _, _, IdName(to), IdName(relName), ExpandAll) =>
        allocate(source, pipelineInfo)
        val newPipelineInfo = pipelineInfo.deepClone()
        newPipelineInfo.newLong(relName)
        newPipelineInfo.newLong(to)
        result += (lp -> newPipelineInfo)

      case Expand(source, _, _, _, IdName(to), IdName(relName), ExpandInto) =>
        allocate(source, pipelineInfo)
        val newPipelineInfo = pipelineInfo.deepClone()
        newPipelineInfo.newLong(relName)
        result += (lp -> newPipelineInfo)

      case p => throw new RegisterAllocationFailed(s"Don't know how to handle $p")
    }

    val allocations = PipelineInformation.empty
    allocate(lp, allocations)

    result.toMap
  }
}

class RegisterAllocationFailed(str: String) extends InternalException(str)