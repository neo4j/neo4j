/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.ResourceManagerFactory
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedParallel
import org.neo4j.cypher.internal.compiler.ExecutionModel.BatchedSingleThreaded
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherParallelRuntimeConfigOption
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.ImmutablePlanningAttributes
import org.neo4j.cypher.internal.planner.spi.NoPreferenceIndexComparatorFactory
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.QueryRuntimeConfig
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport.WorkloadMode
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.query.CompilerInfo
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.factory.KernelTransactionFactory
import org.neo4j.kernel.impl.locking.LockManager
import org.neo4j.kernel.impl.query.ChainableQuerySubscriberProbe
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory
import org.neo4j.kernel.impl.query.NonRecordingQuerySubscriber
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.QuerySubscriberProbe
import org.neo4j.kernel.impl.query.RecordingQuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.WrappingTransactionalContextFactory
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.InternalLogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util.Collections
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

/**
 * This class contains various ugliness needed to perform physical compilation
 * and then execute a query.
 */
class RuntimeTestSupport[CONTEXT <: RuntimeContext](
  graphDb: GraphDatabaseService,
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  workloadMode: WorkloadMode,
  logProvider: InternalLogProvider,
  debugOptions: CypherDebugOptions = CypherDebugOptions.default,
  val defaultTransactionType: Type = Type.EXPLICIT
) {

  private val cypherGraphDb = new GraphDatabaseCypherService(graphDb)
  private val lifeSupport = new LifeSupport
  private val resolver: DependencyResolver = cypherGraphDb.getDependencyResolver

  val runtimeContextManager: TestRuntimeContextManager[CONTEXT] =
    edition.newRuntimeContextManager(resolver, lifeSupport, logProvider)
  private val monitors = resolver.resolveDependency(classOf[Monitors])

  private val kernelTransactionFactory =
    resolver.resolveDependency(classOf[KernelTransactionFactory])

  private val contextFactory = new WrappingTransactionalContextFactory(
    Neo4jTransactionalContextFactory.create(() => cypherGraphDb, kernelTransactionFactory, edition.databaseMode),
    wrapTransactionContext
  )
  private lazy val txIdStore = resolver.resolveDependency(classOf[TransactionIdStore])
  private lazy val authManager = resolver.resolveDependency(classOf[AuthManager])

  private[this] var _tx: InternalTransaction = _
  protected[this] var _txContext: TransactionalContext = _

  private[this] var runtimeTestParameters: RuntimeTestParameters = RuntimeTestParameters()
  private[this] var isParallel: Boolean = _

  def setRuntimeTestParameters(params: RuntimeTestParameters, parallelExecution: Boolean): Unit = {
    runtimeTestParameters = params
    isParallel = parallelExecution
  }

  private def createQuerySubscriberProbe(params: RuntimeTestParameters): QuerySubscriberProbe = {
    var probe: ChainableQuerySubscriberProbe = null

    def addProbe(nextProbe: QuerySubscriberProbe): Unit = {
      val nextChainable = new ChainableQuerySubscriberProbe(nextProbe)
      probe =
        if (probe == null) {
          nextChainable
        } else {
          probe.chain(nextChainable)
        }
    }

    // Slow sleeping subscriber probe
    if (params.sleepSubscriber.isDefined) {
      addProbe(new QuerySubscriberProbe {
        private[this] var count: Long = 0L

        override def onRecordCompleted(): Unit = {
          count += 1L;
          val sleepPerNRows = params.sleepSubscriber.get
          if (count % sleepPerNRows.perNRows == 0) {
            try {
              Thread.sleep(0L, sleepPerNRows.sleepNanos)
            } catch {
              case e: InterruptedException => // Ignore
            }
          }
        }
      })
    }

    // Slow busy-waiting subscriber probe
    if (params.busySubscriber) {
      addProbe(new QuerySubscriberProbe {
        override def onRecordCompleted(): Unit = {
          var i = 0
          while (i < 1000000000) {
            Thread.onSpinWait()
            i += 1
          }
        }
      })
    }

    // Print progress probe
    if (params.printProgress.isDefined) {
      val printEveryNRows = params.printProgress.get
      addProbe(new QuerySubscriberProbe {
        private[this] var count: Long = 0L

        override def onRecordCompleted(): Unit = {
          count += 1L;
          if (count % printEveryNRows.everyNRows == 0) {
            val printCount = if (printEveryNRows.printRowCount) count.toString else ""
            print(s"${printEveryNRows.messagePrefix}$printCount${printEveryNRows.messageSuffix}")
          }
        }
      })
    }

    // Print config probe
    if (params.printConfig) {
      addProbe(new QuerySubscriberProbe {
        var shouldPrint: Boolean = false

        override def onResultCompleted(statistics: QueryStatistics): Unit = {
          printConfig()
        }

        override def onError(throwable: Throwable): Unit = {
          printConfig()
        }

        private def printConfig(): Unit = {
          val nl = System.lineSeparator()
          if (shouldPrint) {
            print(s"${nl}Test config:${nl}${edition.configs.mkString(nl)}${nl}${nl}${nl}")
            shouldPrint = false
          }
        }
      })
    }

    // Kill transaction probe
    params.killTransactionAfterRows match {
      case Some(n) =>
        addProbe(new QuerySubscriberProbe {
          private[this] var count: Long = 0L

          override def onRecordCompleted(): Unit = {
            count += 1L;
            if (count == n) {
              _tx.terminate()
            }
          }
        })

      case None => // Do nothing
    }
    probe
  }

  def start(): Unit = {
    lifeSupport.init()
    lifeSupport.start()
  }

  def stop(): Unit = {
    lifeSupport.stop()
    lifeSupport.shutdown()
  }

  def startTx(transactionType: KernelTransaction.Type = defaultTransactionType): Unit = {
    _tx = cypherGraphDb.beginTransaction(transactionType, LoginContext.AUTH_DISABLED)
    _txContext = contextFactory.newContext(
      _tx,
      "<<queryText>>",
      VirtualValues.EMPTY_MAP,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
  }

  def newTx(transactionType: KernelTransaction.Type = defaultTransactionType)
    : (InternalTransaction, TransactionalContext) = {
    val tx = cypherGraphDb.beginTransaction(transactionType, LoginContext.AUTH_DISABLED)
    val txContext = contextFactory.newContext(
      tx,
      "<<queryText>>",
      VirtualValues.EMPTY_MAP,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
    (tx, txContext)
  }

  def restartTx(transactionType: KernelTransaction.Type = defaultTransactionType): Unit = {
    _txContext.close()
    _tx.commit()
    _tx = cypherGraphDb.beginTransaction(transactionType, LoginContext.AUTH_DISABLED)
    _txContext = contextFactory.newContext(
      _tx,
      "<<queryText>>",
      VirtualValues.EMPTY_MAP,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
  }

  def rollbackAndRestartTx(transactionType: KernelTransaction.Type = defaultTransactionType): Unit = {
    _txContext.close()
    _tx.rollback()
    _tx = cypherGraphDb.beginTransaction(transactionType, LoginContext.AUTH_DISABLED)
    _txContext = contextFactory.newContext(
      _tx,
      "<<queryText>>",
      VirtualValues.EMPTY_MAP,
      QueryExecutionConfiguration.DEFAULT_CONFIG
    )
  }

  def stopTx(): Unit = {
    _txContext.close()
    _tx.close()
  }

  def startNewTx(): InternalTransaction = {
    cypherGraphDb.beginTransaction(defaultTransactionType, LoginContext.AUTH_DISABLED)
  }

  def getLastClosedTransactionId: Long = {
    txIdStore.getHighestGapFreeClosedTransactionId
  }

  def tx: InternalTransaction = _tx
  def txContext: TransactionalContext = _txContext

  def locks: LockManager = cypherGraphDb.getDependencyResolver.resolveDependency(classOf[LockManager])

  // RuntimeExecutionSupport

  def buildPlan(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): ExecutionPlan = {
    val queryContext = newQueryContext(_txContext, queryConfig)
    try {
      compileWithTx(
        logicalQuery,
        runtime,
        queryContext,
        testPlanCombinationRewriterHints
      )._1
    } finally {
      queryContext.resources.close()
    }
  }

  def buildPlanAndContext(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint],
    queryConfig: QueryRuntimeConfig
  ): (ExecutionPlan, CONTEXT) = {
    val queryContext = newQueryContext(_txContext, queryConfig)
    compileWithTx(logicalQuery, runtime, queryContext, testPlanCombinationRewriterHints)
  }

  /**
   * NOTE: This has some default values, because it is also used directly from LogicalPlanFuzzTesting,
   *       alongside RuntimeTestSupportExecution like the rest of the execution methods.
   */
  def executeAndConsumeTransactionally(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    parameters: Map[String, Any],
    profileAssertion: Option[QueryProfile => Unit] = None
  ): PlanRunner[IndexedSeq[Array[AnyValue]]] =
    PlanRunner.empty
      .withPlan(logicalQuery, runtime, Set.empty)
      .withParams(parameters)
      .withTransaction {
        cypherGraphDb.beginTransaction(Type.EXPLICIT, LoginContext.AUTH_DISABLED)
      }
      .recording
      .copy(profile = profileAssertion.isDefined)
      .mapResult { result =>
        val seq = result.awaitAll()
        profileAssertion.foreach(_(result.runtimeResult.queryProfile()))
        result.runtimeResult.close()
        seq
      }

  private def planDescriptionBuilder(logicalQuery: LogicalQuery, executionPlan: ExecutionPlan) =
    PlanDescriptionBuilder(
      executionPlan.rewrittenPlan.getOrElse(logicalQuery.logicalPlan),
      IDPPlannerName,
      logicalQuery.readOnly,
      ImmutablePlanningAttributes.EffectiveCardinalities(logicalQuery.effectiveCardinalities),
      debugOptions.rawCardinalitiesEnabled,
      debugOptions.renderDistinctnessEnabled,
      debugOptions.renderNestedPlanExpressions,
      ImmutablePlanningAttributes.ProvidedOrders(logicalQuery.providedOrders),
      executionPlan,
      renderPlanDescription = false,
      CypherVersion.Legacy.legacyVersion(),
      explainScopeOpt = None,
      cypherPlannerVersion = None
    )

  def explainDescription(logicalQuery: LogicalQuery, executionPlan: ExecutionPlan): InternalPlanDescription =
    planDescriptionBuilder(logicalQuery, executionPlan).explain()

  def profileDescription(
    logicalQuery: LogicalQuery,
    executionPlan: ExecutionPlan,
    profile: QueryProfile
  ): InternalPlanDescription =
    planDescriptionBuilder(logicalQuery, executionPlan).profile(profile)

  sealed trait Plan

  object Plan {

    case class Unbuilt(
      query: LogicalQuery,
      runtime: CypherRuntime[CONTEXT],
      testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
    ) extends Plan

    case class Built(plan: ExecutionPlan) extends Plan
  }

  case class PlanRunner[RESULT](
    plan: Plan,
    input: InputDataStream,
    subscriber: QuerySubscriber,
    profile: Boolean,
    prePopulateResults: Boolean,
    parameters: Map[String, Any],
    queryConfig: QueryRuntimeConfig,
    resultMapper: (CONTEXT, RuntimeResult) => RESULT,
    transaction: TransactionSelection
  ) {

    def withPlan(plan: ExecutionPlan): PlanRunner[RESULT] =
      copy(plan = Plan.Built(plan))

    def withPlan(
      query: LogicalQuery,
      runtime: CypherRuntime[CONTEXT],
      testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
    ): PlanRunner[RESULT] =
      copy(plan = Plan.Unbuilt(query, runtime, testPlanCombinationRewriterHints))

    def withPlan(
      query: LogicalQuery,
      runtime: CypherRuntime[CONTEXT]
    ): PlanRunner[RESULT] =
      withPlan(query, runtime, Set.empty)

    def withPlan(query: LogicalQuery): PlanRunner[RESULT] =
      withPlan(query, runtime)

    def recording(implicit ev: RESULT =:= RuntimeResult): PlanRunner[RecordingRuntimeResult] = {
      val sub = new RecordingQuerySubscriber(createQuerySubscriberProbe(runtimeTestParameters))
      withSubscriber(sub)
        .mapResult(RecordingRuntimeResult(_, sub, runtimeTestParameters.resultConsumptionController))
    }

    def nonRecording(implicit ev: RESULT =:= RuntimeResult): PlanRunner[NonRecordingRuntimeResult] = {
      val sub = new NonRecordingQuerySubscriber(createQuerySubscriberProbe(runtimeTestParameters))
      withSubscriber(sub)
        .mapResult(NonRecordingRuntimeResult(
          _,
          sub,
          runtimeTestParameters.resultConsumptionController
        ))
    }

    def withImplicitTx(implicitTx: Boolean): PlanRunner[RESULT] =
      if (implicitTx) {
        withTransaction {
          cypherGraphDb.beginTransaction(Type.IMPLICIT, LoginContext.AUTH_DISABLED)
        }
      } else this

    def noValues: PlanRunner[RESULT] = copy(prePopulateResults = false)

    def withSubscriber(sub: QuerySubscriber): PlanRunner[RESULT] = copy(subscriber = sub)

    def withInput(input: InputDataStream): PlanRunner[RESULT] = copy(input = input)

    def withParams(params: Map[String, Any]): PlanRunner[RESULT] = copy(parameters = params)

    def withConfig(queryConfig: QueryRuntimeConfig): PlanRunner[RESULT] = copy(queryConfig = queryConfig)

    def profiling: PlanRunner[RESULT] = copy(profile = true)

    def withContext: PlanRunner[(RESULT, CONTEXT)] =
      copy(resultMapper = (c, res) => (resultMapper(c, res), c))

    def mapResult[RESULT2](f: RESULT => RESULT2): PlanRunner[RESULT2] =
      copy(resultMapper = (c, res) => f(resultMapper(c, res)))

    // note: call-by-value, will create a new transaction and close it after run()
    def withTransaction(tx: => InternalTransaction): PlanRunner[RESULT] =
      copy(transaction = TransactionSelection.Owned(() => tx))

    def executeAs(username: String, password: String): PlanRunner[RESULT] =
      withTransaction {
        val lgCtx =
          authManager.login(AuthToken.newBasicAuthToken(username, password), ClientConnectionInfo.EMBEDDED_CONNECTION)
        cypherGraphDb.beginTransaction(Type.EXPLICIT, lgCtx)
      }

    def withTimeout(duration: FiniteDuration): PlanRunner[RESULT] =
      withTransaction {
        cypherGraphDb.beginTransaction(
          Type.EXPLICIT,
          LoginContext.AUTH_DISABLED,
          ClientConnectionInfo.EMBEDDED_CONNECTION,
          duration.toSeconds.toInt,
          TimeUnit.SECONDS
        )
      }

    private def getPlan(txContext: TransactionalContext): ExecutionPlan =
      plan match {
        case Plan.Unbuilt(query, runtime, testPlanCombinationRewriterHints) =>
          val queryContext = newQueryContext(txContext, queryConfig)
          try {
            compileWithTx(
              query.copy(doProfile = profile),
              runtime,
              queryContext,
              testPlanCombinationRewriterHints
            )._1
          } finally {
            queryContext.resources.close()
          }
        case Plan.Built(plan) =>
          plan

        case PlanRunner.NoPlan =>
          throw new IllegalArgumentException("No plan specified for PlanRunner.")
      }

    def run(): RESULT = {
      transaction match {
        case TransactionSelection.Shared => doRun(_tx, _txContext)
        case TransactionSelection.Owned(create) =>
          val tx = create()
          val txContext = contextFactory.newContext(
            tx,
            "<<queryText>>",
            VirtualValues.EMPTY_MAP,
            QueryExecutionConfiguration.DEFAULT_CONFIG
          )
          try {
            doRun(tx, txContext)
          } catch {
            case NonFatal(e) =>
              txContext.close()
              tx.close()
              throw e
          }
      }
    }

    private def doRun(tx: InternalTransaction, txContext: TransactionalContext) = {
      val executableQuery = getPlan(txContext)
      val defaultLanguage = CypherVersion.Legacy.legacyVersion()
      txContext.executingQuery().setCompilerInfoForTesting(new CompilerInfo(
        "NO PLANNER",
        "v2026_04",
        executableQuery.runtimeName,
        Collections.emptyList(),
        defaultLanguage
      ))
      val queryContext = newQueryContext(txContext, queryConfig, executableQuery.threadSafeExecutionResources())
      val runtimeContext = newRuntimeContext(queryContext, defaultLanguage)

      val executionMode = if (profile) ProfileMode else NormalMode
      val (keys, values) =
        parameters.mapValues {
          case m: MapValue  => m
          case m: Map[_, _] => VirtualValues.map(m.keys.map(_.toString).toArray, m.values.map(ValueUtils.of).toArray)
          case v            => ValueUtils.of(v)
        }.unzip match { case (a, b) => (a.toArray, b.toArray[AnyValue]) }

      val paramsMap = VirtualValues.map(keys, values)
      val result =
        executableQuery.run(queryContext, executionMode, paramsMap, prePopulateResults, input, subscriber)

      val assertAllReleased = workloadMode match {
        case WorkloadMode.On => () => ()
        case WorkloadMode.Off => () => {
            runtimeContextManager.waitForWorkersToIdle(5000)
            runtimeContextManager.assertAllReleased()
          }
      }
      resultMapper(
        runtimeContext,
        new ClosingRuntimeTestResult(
          result,
          tx,
          txContext,
          queryContext.resources,
          subscriber,
          assertAllReleased,
          closeTx = transaction.isInstanceOf[TransactionSelection.Owned]
        )
      )
    }

  }

  object PlanRunner {
    private object NoPlan extends Plan

    def empty: PlanRunner[RuntimeResult] = PlanRunner(
      NoPlan,
      NoInput,
      QuerySubscriber.DO_NOTHING_SUBSCRIBER,
      profile = false,
      prePopulateResults = true,
      parameters = Map.empty,
      queryConfig = runtimeContextManager.defaultQueryRuntimeConfig,
      resultMapper = (_, res) => res,
      transaction = TransactionSelection.Shared
    )
  }

  sealed trait TransactionSelection

  object TransactionSelection {

    /** refers to the tx and txContext shared by the base class */
    case object Shared extends TransactionSelection

    /** a transaction that will be created and closed by the runner */
    case class Owned(create: () => InternalTransaction) extends TransactionSelection
  }

  // PRIVATE EXECUTE HELPER METHODS

  private def compileWithTx(
    logicalQuery: LogicalQuery,
    runtime: CypherRuntime[CONTEXT],
    queryContext: QueryContext,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): (ExecutionPlan, CONTEXT) = {
    val defaultLanguage = CypherVersion.Legacy.legacyVersion() // To be replaced with db specific default
    val runtimeContext = newRuntimeContext(queryContext, defaultLanguage)
    val rewrittenLogicalQuery =
      rewriteLogicalQuery(logicalQuery, runtimeContext.anonymousVariableNameGenerator, testPlanCombinationRewriterHints)
    (runtime.compileToExecutable(rewrittenLogicalQuery, runtimeContext, txContext.databaseMode()), runtimeContext)
  }

  protected[spec] def rewriteLogicalQuery(
    logicalQuery: LogicalQuery,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint]
  ): LogicalQuery = {
    runtimeTestParameters.planCombinationRewriter match {
      case Some(rewriterConfig) if testPlanCombinationRewriterHints.nonEmpty =>
        val augmentedRewriterConfig =
          rewriterConfig.copy(hints = rewriterConfig.hints.union(testPlanCombinationRewriterHints))
        TestPlanCombinationRewriter(augmentedRewriterConfig, logicalQuery, isParallel, anonymousVariableNameGenerator)
      case Some(rewriterConfig) =>
        TestPlanCombinationRewriter(rewriterConfig, logicalQuery, isParallel, anonymousVariableNameGenerator)
      case _ =>
        logicalQuery
    }
  }

  // CONTEXTS

  protected def wrapTransactionContext(ctx: TransactionalContext): TransactionalContext = ctx

  private def selectExecutionModel(queryOptions: QueryOptions): ExecutionModel = {
    if (RuntimeTestSuite.isParallel(runtime)) {
      queryOptions.queryOptions.parallelRuntimeConfigOption match {
        case CypherParallelRuntimeConfigOption.none =>
          BatchedParallel(
            runtimeContextManager.cypherConfig.pipelinedBatchSizeSmall,
            runtimeContextManager.cypherConfig.pipelinedBatchSizeBig,
            providedOrderPreserving = false
          )
        case CypherParallelRuntimeConfigOption.leverageOrder =>
          BatchedParallel(
            runtimeContextManager.cypherConfig.pipelinedBatchSizeSmall,
            runtimeContextManager.cypherConfig.pipelinedBatchSizeBig,
            providedOrderPreserving = true
          )
      }
    } else if (RuntimeTestSuite.isPipelined(runtime)) {
      BatchedSingleThreaded(
        runtimeContextManager.cypherConfig.pipelinedBatchSizeSmall,
        runtimeContextManager.cypherConfig.pipelinedBatchSizeBig
      )
    } else {
      ExecutionModel.Volcano
    }
  }

  protected def newRuntimeContext(queryContext: QueryContext, dbDefaultLanguage: CypherVersion): CONTEXT = {

    val queryOptions = runtimeContextManager.defaultQueryOptions.copy(defaultLanguage = dbDefaultLanguage)
    val executionModel = selectExecutionModel(queryOptions)

    runtimeContextManager.create(
      queryOptions.resolvedLanguage,
      queryContext,
      _txContext,
      MasterCompiler.CLOCK,
      debugOptions,
      compileExpressions = queryOptions.useCompiledExpressions,
      materializedEntitiesMode = queryOptions.materializedEntitiesMode,
      operatorEngine = queryOptions.queryOptions.operatorEngine,
      interpretedPipesFallback = queryOptions.queryOptions.interpretedPipesFallback,
      anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
      executionModel,
      indexComparatorFactory = NoPreferenceIndexComparatorFactory
    )
  }

  private def newQueryContext(
    txContext: TransactionalContext,
    queryConfig: QueryRuntimeConfig,
    maybeExecutionResources: Option[ResourceManagerFactory] = None
  ): QueryContext = {
    val resourceManager = maybeExecutionResources match {
      case Some(resourceManagerFactory) => resourceManagerFactory(ResourceMonitor.NOOP)
      case None => new ResourceManager(ResourceMonitor.NOOP, txContext.kernelTransaction().memoryTracker())
    }

    new TransactionBoundQueryContext(
      TransactionalContextWrapper(txContext),
      resourceManager,
      queryConfig = queryConfig
    )(
      monitors.newMonitor(classOf[IndexSearchMonitor])
    )
  }

  def waitForWorkersToIdle(timeoutMs: Int): Unit = {
    runtimeContextManager.waitForWorkersToIdle(timeoutMs)
  }
}

object RuntimeTestSupport {
  sealed trait WorkloadMode

  object WorkloadMode {
    case object On extends WorkloadMode
    case object Off extends WorkloadMode
  }
}
