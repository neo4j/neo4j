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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.{CypherRuntime, LogicalQuery, MasterCompiler, RuntimeContext}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.Transaction
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.util.DefaultValueMapper
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.BeforeAndAfterAll

/**
  * This class contains various ugliness needed to perform physical compilation
  * and then execute a query.
  */
class RuntimeTestSupport[CONTEXT <: RuntimeContext](val graphDb: GraphDatabaseService,
                                                    val edition: Edition[CONTEXT]
                                                   ) extends CypherFunSuite with BeforeAndAfterAll
{
  val cypherGraphDb = new GraphDatabaseCypherService(graphDb)
  private val resolver: DependencyResolver = cypherGraphDb.getDependencyResolver
  private val runtimeContextCreator = edition.runtimeContextCreator(resolver)
  private val monitors = resolver.resolveDependency(classOf[Monitors])
  private val contextFactory = Neo4jTransactionalContextFactory.create(cypherGraphDb)
  private val spi: EmbeddedProxySPI = resolver.resolveDependency(classOf[EmbeddedProxySPI], DependencyResolver.SelectionStrategy.SINGLE)

  val valueMapper = new DefaultValueMapper(spi)

  override def afterAll(): Unit = {
    graphDb.shutdown()
    super.afterAll()
  }

  def run[RESULT](logicalQuery: LogicalQuery,
                  runtime: CypherRuntime[CONTEXT],
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT): RESULT = {
    val tx = cypherGraphDb.beginTransaction(Transaction.Type.`implicit`, LoginContext.AUTH_DISABLED)
    val queryContext = newQueryContext(tx)
    val runtimeContext = newRuntimeContext(tx)

    val executableQuery = runtime.compileToExecutable(logicalQuery, runtimeContext)
    val result = executableQuery.run(queryContext, false, VirtualValues.EMPTY_MAP, prePopulateResults = true, input)
    resultMapper(runtimeContext, result)
  }

  def newRuntimeContext(tx: InternalTransaction): CONTEXT = {
    val contextFactory = Neo4jTransactionalContextFactory.create(cypherGraphDb)
    val txContext = TransactionalContextWrapper(contextFactory.newContext(tx, "<<queryText>>", VirtualValues.EMPTY_MAP))
    val queryContext = new TransactionBoundQueryContext(txContext)(monitors.newMonitor(classOf[IndexSearchMonitor]))
    runtimeContextCreator.create(queryContext,
                                 tx.kernelTransaction().schemaRead(),
                                 MasterCompiler.CLOCK,
                                 Set.empty,
                                 compileExpressions = false)
  }

  def newQueryContext(tx: InternalTransaction): QueryContext = {
    val txContext = TransactionalContextWrapper(contextFactory.newContext(tx, "<<queryText>>", VirtualValues.EMPTY_MAP))
    new TransactionBoundQueryContext(txContext)(monitors.newMonitor(classOf[IndexSearchMonitor]))
  }
}
