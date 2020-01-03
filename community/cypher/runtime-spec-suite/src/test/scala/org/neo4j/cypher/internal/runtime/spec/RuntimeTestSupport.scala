/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.{InputDataStream, NormalMode, ProfileMode, QueryContext, ResourceManager, ResourceMonitor}
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, QuerySubscriber, TransactionalContext}
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.VirtualValues

/**
  * This class contains various ugliness needed to perform physical compilation
  * and then execute a query.
  */
class RuntimeTestSupport[CONTEXT <: RuntimeContext](val graphDb: GraphDatabaseService,
                                                    val edition: Edition[CONTEXT],
                                                    val workloadMode: Boolean,
                                                    val logProvider: LogProvider
                                                   ) extends CypherFunSuite {

  private val cypherGraphDb = new GraphDatabaseCypherService(graphDb)
  private val lifeSupport = new LifeSupport
  private val resolver: DependencyResolver = cypherGraphDb.getDependencyResolver
  protected val runtimeContextManager: RuntimeContextManager[CONTEXT] = edition.newRuntimeContextManager(resolver, lifeSupport, logProvider)
  private val monitors = resolver.resolveDependency(classOf[Monitors])
  private val contextFactory = Neo4jTransactionalContextFactory.create(cypherGraphDb)

  private var _tx: InternalTransaction = _
  private var txContext: TransactionalContext = _

  def start(): Unit = {
    lifeSupport.init()
    lifeSupport.start()
  }

  def stop(): Unit = {
    lifeSupport.stop()
    lifeSupport.shutdown()
  }

  def startTx(): Unit = {
    _tx = cypherGraphDb.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    txContext = contextFactory.newContext(_tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
  }

  def restartTx(): Unit = {
    txContext.close()
    _tx.commit()
    _tx = cypherGraphDb.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    txContext = contextFactory.newContext(_tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
  }

  def stopTx(): Unit = {
    txContext.close()
    _tx.close()
  }

  def tx: InternalTransaction = _tx

  def run[RESULT](logicalQuery: LogicalQuery,
                  runtime: CypherRuntime[CONTEXT],
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber,
                  profile: Boolean): RESULT = {
    DebugLog.log("RuntimeTestSupport.run(...)")
    run(compile(logicalQuery, runtime), input, resultMapper, subscriber, profile)
  }

  def runTransactionally[RESULT](logicalQuery: LogicalQuery,
                  runtime: CypherRuntime[CONTEXT],
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber,
                  profile: Boolean): RESULT = {
    DebugLog.log("RuntimeTestSupport.run(...)")
    val tx = cypherGraphDb.beginTransaction(Type.explicit, LoginContext.AUTH_DISABLED)
    val txContext = contextFactory.newContext(tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
    val queryContext = newQueryContext(txContext)
    try {
      val executionPlan = compileWithTx(logicalQuery, runtime, queryContext)
      runWithTx(executionPlan, input, resultMapper, subscriber, profile, tx, txContext)
    } finally {
      txContext.close()
      tx.close()
    }
  }

  def run[RESULT](executableQuery: ExecutionPlan,
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber,
                  profile: Boolean): RESULT = {
    runWithTx(executableQuery, input, resultMapper, subscriber, profile, _tx, txContext)
  }

  def runWithTx[RESULT](executableQuery: ExecutionPlan,
                        input: InputDataStream,
                        resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                        subscriber: QuerySubscriber,
                        profile: Boolean,
                        tx: InternalTransaction,
                        txContext: TransactionalContext): RESULT = {
    val queryContext = newQueryContext(txContext, executableQuery.threadSafeExecutionResources())
    val runtimeContext = newRuntimeContext(queryContext)

    val executionMode = if(profile) ProfileMode else NormalMode
    val result = executableQuery.run(queryContext, executionMode, VirtualValues.EMPTY_MAP, prePopulateResults = true, input, subscriber)
    val assertAllReleased =
      if (!workloadMode) runtimeContextManager.assertAllReleased _ else () => ()
    resultMapper(runtimeContext, new ClosingRuntimeResult(result, tx, txContext, queryContext.resources, subscriber, assertAllReleased))
  }

  def compile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]): ExecutionPlan = {
    compileWithTx(logicalQuery, runtime, newQueryContext(txContext))
  }

  def compileWithTx(logicalQuery: LogicalQuery,
                    runtime: CypherRuntime[CONTEXT],
                    queryContext: QueryContext): ExecutionPlan = {
    val runtimeContext = newRuntimeContext(queryContext)
    runtime.compileToExecutable(logicalQuery, runtimeContext)
  }

  protected def newRuntimeContext(queryContext: QueryContext): CONTEXT = {

    val cypherConfiguration: CypherConfiguration = edition.cypherConfig()

    val queryOptions = PreParser.queryOptions(Seq.empty,
      InputPosition.NONE,
      isPeriodicCommit = false,
      cypherConfiguration.version,
      cypherConfiguration.planner,
      cypherConfiguration.runtime,
      cypherConfiguration.expressionEngineOption,
      cypherConfiguration.operatorEngine,
      cypherConfiguration.interpretedPipesFallback)

    runtimeContextManager.create(queryContext,
                                 queryContext.transactionalContext.transaction.schemaRead(),
                                 MasterCompiler.CLOCK,
                                 Set.empty,
                                 compileExpressions = queryOptions.useCompiledExpressions,
                                 materializedEntitiesMode = queryOptions.materializedEntitiesMode,
                                 operatorEngine = queryOptions.operatorEngine,
                                 interpretedPipesFallback = queryOptions.interpretedPipesFallback)
  }

  private def newQueryContext(txContext: TransactionalContext, maybeExecutionResources: Option[(CursorFactory, ResourceManagerFactory)] = None): QueryContext = {
    val (threadSafeCursorFactory, resourceManager) = maybeExecutionResources match {
      case Some((tFactory, rFactory)) => (tFactory, rFactory(ResourceMonitor.NOOP))
      case None => (null, new ResourceManager(ResourceMonitor.NOOP))
    }

    new TransactionBoundQueryContext(TransactionalContextWrapper(txContext, threadSafeCursorFactory), resourceManager)(monitors.newMonitor(classOf[IndexSearchMonitor]))
  }
}
