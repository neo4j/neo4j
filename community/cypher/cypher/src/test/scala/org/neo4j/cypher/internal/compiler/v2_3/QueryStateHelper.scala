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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ExternalResource, NullPipeDecorator, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.spi.v2_3.TransactionBoundQueryContext
import org.neo4j.cypher.internal.spi.v2_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

object QueryStateHelper {
  def empty: QueryState = emptyWith()

  def emptyWith(db: GraphDatabaseService = null, query: QueryContext = null, resources: ExternalResource = null,
                params: Map[String, Any] = Map.empty, decorator: PipeDecorator = NullPipeDecorator) =
    new QueryState(query = query, resources = resources, params = params, decorator = decorator)

  def queryStateFrom(db: GraphDatabaseAPI, tx: Transaction, params: Map[String, Any] = Map.empty): QueryState = {
    val statement: Statement = db.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
    val searchMonitor = new KernelMonitors().newMonitor(classOf[IndexSearchMonitor])
    val context = new TransactionBoundQueryContext(db, tx, isTopLevelTx = true, statement)(searchMonitor)
    emptyWith(db = db, query = context, params = params)
  }

  def countStats(q: QueryState) = q.withQueryContext(query = new UpdateCountingQueryContext(q.query))
}
