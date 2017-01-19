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
package org.neo4j.cypher.internal.compiler.v3_2

import java.util.Collections

import org.neo4j.cypher.internal.compiler.v3_2.pipes.{ExternalCSVResource, NullPipeDecorator, PipeDecorator, QueryState}
import org.neo4j.cypher.internal.compiler.v3_2.spi.{QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.internal.spi.v3_2.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.spi.v3_2.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.coreapi.{InternalTransaction, PropertyContainerLocker}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.collection.mutable

object QueryStateHelper {
  def empty: QueryState = newWith()

  def newWith(db: GraphDatabaseQueryService = null, query: QueryContext = null, resources: ExternalCSVResource = null,
              params: Map[String, Any] = Map.empty, decorator: PipeDecorator = NullPipeDecorator) =
    new QueryState(query = query, resources = resources, params = params, decorator = decorator, triadicState = mutable.Map.empty, repeatableReads = mutable.Map.empty)

  private val locker: PropertyContainerLocker = new PropertyContainerLocker

  def queryStateFrom(db: GraphDatabaseQueryService, tx: InternalTransaction, params: Map[String, Any] = Map.empty): QueryState = {
    val searchMonitor = new KernelMonitors().newMonitor(classOf[IndexSearchMonitor])
    val contextFactory = Neo4jTransactionalContextFactory.create(db, locker)
    val transactionalContext = TransactionalContextWrapper(contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, tx, "X", Collections.emptyMap()))
    val queryContext = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
    newWith(db = db, query = queryContext, params = params)
  }

  def countStats(q: QueryState) = q.withQueryContext(query = new UpdateCountingQueryContext(q.query))
}
