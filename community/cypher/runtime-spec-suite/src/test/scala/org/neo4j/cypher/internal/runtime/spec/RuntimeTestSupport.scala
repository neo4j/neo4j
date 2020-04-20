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
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.CypherConfiguration
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.PreParser
import org.neo4j.cypher.internal.ResourceManagerFactory
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeContextManager
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryTransactionalContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

/**
 * This class contains various ugliness needed to perform physical compilation
 * and then execute a query.
 */
class RuntimeTestSupport[CONTEXT <: RuntimeContext](val graphDb: GraphDatabaseService,
                                                    val edition: Edition[CONTEXT],
                                                    val workloadMode: Boolean,
                                                    val logProvider: LogProvider
                                                   ) extends RuntimeExecutionSupport[CONTEXT] {

  private val cypherGraphDb = new GraphDatabaseCypherService(graphDb)
  private val lifeSupport = new LifeSupport
  private val resolver: DependencyResolver = cypherGraphDb.getDependencyResolver
  protected val runtimeContextManager: RuntimeContextManager[CONTEXT] = edition.newRuntimeContextManager(resolver, lifeSupport, logProvider)
  private val monitors = resolver.resolveDependency(classOf[Monitors])
  private val contextFactory = Neo4jTransactionalContextFactory.create(cypherGraphDb)

  private var _tx: InternalTransaction = _
  private var _txContext: TransactionalContext = _

  def start(): Unit = {
    lifeSupport.init()
    lifeSupport.start()
  }

  def stop(): Unit = {
    lifeSupport.stop()
    lifeSupport.shutdown()
  }

  def startTx(): Unit = {
    _tx = cypherGraphDb.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    _txContext = contextFactory.newContext(_tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
  }

  def restartTx(): Unit = {
    _txContext.close()
    _tx.commit()
    _tx = cypherGraphDb.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    _txContext = contextFactory.newContext(_tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
  }

  def stopTx(): Unit = {
    _txContext.close()
    _tx.close()
  }

  def tx: InternalTransaction = _tx
  def txContext: TransactionalContext = _txContext

  override def buildPlan(logicalQuery: LogicalQuery,
                         runtime: CypherRuntime[CONTEXT]): ExecutionPlan =
    compileWithTx(logicalQuery, runtime, newQueryContext(_txContext))._1

  override def buildPlanAndContext(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT]): (ExecutionPlan, CONTEXT) =
    compileWithTx(logicalQuery, runtime, newQueryContext(_txContext))

  override def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = run(executablePlan, NoInput, (_, result) => result, subscriber, profile = false)
    RecordingRuntimeResult(result, subscriber)
  }

  override def execute(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       inputStream: InputDataStream): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runLogical(logicalQuery, runtime, inputStream, (_, result) => result, subscriber, profile = false)
    RecordingRuntimeResult(result, subscriber)
  }

  override def execute(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       input: InputDataStream,
                       subscriber: QuerySubscriber): RuntimeResult = runLogical(logicalQuery, runtime, input, (_, result) => result, subscriber, profile = false)


  override def executeAndConsumeTransactionally(logicalQuery: LogicalQuery,
                                                runtime: CypherRuntime[CONTEXT],
                                                parameters: Map[String, Any] = Map.empty): IndexedSeq[Array[AnyValue]] = {
    val subscriber = new RecordingQuerySubscriber
    runTransactionally(logicalQuery, runtime, NoInput, (_, result) => {
      val recordingRuntimeResult = RecordingRuntimeResult(result, subscriber)
      val seq = recordingRuntimeResult.awaitAll()
      recordingRuntimeResult.runtimeResult.close()
      seq
    }, subscriber, parameters)
  }

  override def profile(logicalQuery: LogicalQuery,
                       runtime: CypherRuntime[CONTEXT],
                       inputDataStream: InputDataStream = NoInput): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runLogical(logicalQuery, runtime, inputDataStream, (_, result) => result, subscriber, profile = true)
    RecordingRuntimeResult(result, subscriber)
  }

  override def profileNonRecording(logicalQuery: LogicalQuery,
                                   runtime: CypherRuntime[CONTEXT],
                                   inputDataStream: InputDataStream = NoInput): NonRecordingRuntimeResult = {
    val subscriber = new NonRecordingQuerySubscriber
    val result = runLogical(logicalQuery, runtime, inputDataStream, (_, result) => result, subscriber, profile = true)
    NonRecordingRuntimeResult(result, subscriber)
  }

  override def executeAndContext(logicalQuery: LogicalQuery,
                                 runtime: CypherRuntime[CONTEXT],
                                 input: InputValues
                                ): (RecordingRuntimeResult, CONTEXT) = {
    val subscriber = new RecordingQuerySubscriber
    val (result, context) = runLogical(logicalQuery, runtime, input.stream(), (context, result) => (result, context), subscriber, profile = false)
    (RecordingRuntimeResult(result, subscriber), context)
  }

  override def executeAndExplain(logicalQuery: LogicalQuery,
                                 runtime: CypherRuntime[CONTEXT],
                                 input: InputValues): (RecordingRuntimeResult, InternalPlanDescription) = {
    val subscriber = new RecordingQuerySubscriber
    val executionPlan = buildPlan(logicalQuery, runtime)
    val result = run(executionPlan, input.stream(), (_, result) => result, subscriber, profile = false, parameters = Map.empty)
    val executionPlanDescription = {
      val planDescriptionBuilder =
        new PlanDescriptionBuilder(executionPlan.rewrittenPlan.getOrElse(logicalQuery.logicalPlan),
                                   IDPPlannerName,
                                   CypherVersion.default,
                                   logicalQuery.readOnly,
                                   logicalQuery.cardinalities,
                                   logicalQuery.providedOrders,
                                   executionPlan)
      planDescriptionBuilder.explain()
    }
    (RecordingRuntimeResult(result, subscriber), executionPlanDescription)
  }

  // PRIVATE EXECUTE HELPER METHODS

  private def runLogical[RESULT](logicalQuery: LogicalQuery,
                         runtime: CypherRuntime[CONTEXT],
                         input: InputDataStream,
                         resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                         subscriber: QuerySubscriber,
                         profile: Boolean,
                         parameters: Map[String, Any] = Map.empty): RESULT = {
    run(buildPlan(logicalQuery, runtime), input, resultMapper, subscriber, profile, parameters)
  }

  private def runTransactionally[RESULT](logicalQuery: LogicalQuery,
                                         runtime: CypherRuntime[CONTEXT],
                                         input: InputDataStream,
                                         resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                                         subscriber: QuerySubscriber,
                                         parameters: Map[String, Any]): RESULT = {
    val tx = cypherGraphDb.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
    val txContext = contextFactory.newContext(tx, "<<queryText>>", VirtualValues.EMPTY_MAP)
    val queryContext = newQueryContext(txContext)
    try {
      val executionPlan = compileWithTx(logicalQuery, runtime, queryContext)._1
      runWithTx(executionPlan, input, resultMapper, subscriber, profile = false, parameters, tx, txContext)
    } finally {
      txContext.close()
      tx.close()
    }
  }

  private def run[RESULT](executableQuery: ExecutionPlan,
                  input: InputDataStream,
                  resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                  subscriber: QuerySubscriber,
                  profile: Boolean,
                  parameters: Map[String, Any] = Map.empty): RESULT = {
    runWithTx(executableQuery, input, resultMapper, subscriber, profile, parameters, _tx, _txContext)
  }

  private def runWithTx[RESULT](executableQuery: ExecutionPlan,
                        input: InputDataStream,
                        resultMapper: (CONTEXT, RuntimeResult) => RESULT,
                        subscriber: QuerySubscriber,
                        profile: Boolean,
                        parameters: Map[String, Any],
                        tx: InternalTransaction,
                        txContext: TransactionalContext): RESULT = {
    val queryContext = newQueryContext(txContext, executableQuery.threadSafeExecutionResources())
    val runtimeContext = newRuntimeContext(queryContext)

    val executionMode = if(profile) ProfileMode else NormalMode
    val (keys, values) = parameters.mapValues(Values.of).unzip match { case (a,b) => (a.toArray, b.toArray[AnyValue]) }
    val paramsMap = VirtualValues.map(keys, values)
    val result = executableQuery.run(queryContext, executionMode, paramsMap, prePopulateResults = true, input, subscriber)
    val assertAllReleased =
      if (!workloadMode) runtimeContextManager.assertAllReleased _ else () => ()
    resultMapper(runtimeContext, new ClosingRuntimeResult(result, tx, txContext, queryContext.resources, subscriber, assertAllReleased))
  }

  private def compileWithTx(logicalQuery: LogicalQuery,
                            runtime: CypherRuntime[CONTEXT],
                            queryContext: QueryContext): (ExecutionPlan, CONTEXT) = {
    val runtimeContext = newRuntimeContext(queryContext)
    (runtime.compileToExecutable(logicalQuery, runtimeContext), runtimeContext)
  }

  // CONTEXTS

  def newQueryTransactionalContext(): QueryTransactionalContext = {
    TransactionalContextWrapper(_txContext, null)
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
