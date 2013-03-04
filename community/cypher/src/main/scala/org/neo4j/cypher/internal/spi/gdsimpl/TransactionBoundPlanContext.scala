/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.MissingIndexException
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.api.StatementContext

class TransactionBoundPlanContext(graph: GraphDatabaseAPI) extends PlanContext {

  val tx: Transaction = graph.beginTx()
  private val ctx: StatementContext = graph
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])
    .getCtxForWriting

  def close(success: Boolean) {
    if (success)
      tx.success()
    else
      tx.failure()
    tx.finish()
  }

  def getIndexRuleId(labelName: String, propertyKey: String) = {
    val labelId = ctx.getLabelId(labelName)
    val propertyKeyId = ctx.getPropertyKeyId(propertyKey)
    ctx.getIndexRule(labelId, propertyKeyId).getId
  }

  def checkNodeIndex(idxName: String) {
    if (!graph.index.existsForNodes(idxName)) throw new MissingIndexException(idxName)
  }

  def checkRelIndex(idxName: String) {
    if (!graph.index.existsForRelationships(idxName)) throw new MissingIndexException(idxName)
  }
}