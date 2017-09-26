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
package org.neo4j.cypher.internal.runtime.vectorized.operators

import java.lang

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.graphdb.{Node, Path, Relationship, Result}

class ProduceResultOperator(pipelineInformation: PipelineInformation) extends MiddleOperator {
  override def operate(iterationState: Iteration, data: Morsel, context: QueryContext, state: QueryState): Unit = {
    val resultRow = new MorselResultRow(data, 0, pipelineInformation, context)
    (0 until data.validRows) foreach { position =>
      resultRow.currentPos = position
      state.visitor.visit(resultRow)
    }
  }
}

class MorselResultRow(var morsel: Morsel,
                      var currentPos: Int,
                      pipelineInformation: PipelineInformation,
                      queryContext: QueryContext) extends Result.ResultRow {
  override def getNode(key: String): Node = {
    val nodeOffset = pipelineInformation.getLongOffsetFor(key)
    val nodeId = morsel.longs(currentPos * pipelineInformation.numberOfLongs + nodeOffset)
    queryContext.nodeOps.getById(nodeId)
  }

  override def getRelationship(key: String): Relationship = ???

  override def get(key: String): AnyRef = ???

  override def getString(key: String): String = ???

  override def getNumber(key: String): Number = ???

  override def getBoolean(key: String): lang.Boolean = ???

  override def getPath(key: String): Path = ???
}