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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.CypherCurrentCompiler.CypherExecutableQuery
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CachedExecutionPlan
import org.neo4j.cypher.internal.cache.CypherQueryCaches.ExecutionPlanCacheKey
import org.neo4j.cypher.internal.compiler.phases.CachableLogicalPlanState
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.ProcedureDbmsAccess
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.SchemaIndexLookupUsage
import org.neo4j.cypher.internal.logical.plans.SchemaLabelIndexUsage
import org.neo4j.cypher.internal.logical.plans.SchemaRelationshipIndexUsage
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.plandescription.PlanDescriptionBuilder
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.EffectiveCardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.planning.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.planning.LogicalPlanResult
import org.neo4j.cypher.internal.result.ClosingExecutionResult
import org.neo4j.cypher.internal.result.Error
import org.neo4j.cypher.internal.result.ExplainExecutionResult
import org.neo4j.cypher.internal.result.FailedExecutionResult
import org.neo4j.cypher.internal.result.Failure
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.result.StandardInternalExecutionResult
import org.neo4j.cypher.internal.result.StandardInternalExecutionResult.NoOuterCloseable
import org.neo4j.cypher.internal.result.Success
import org.neo4j.cypher.internal.result.TaskCloser
import org.neo4j.cypher.internal.runtime.DBMS
import org.neo4j.cypher.internal.runtime.DBMS_READ
import org.neo4j.cypher.internal.runtime.ExplainMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.InternalQueryType
import org.neo4j.cypher.internal.runtime.NormalMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.READ_ONLY
import org.neo4j.cypher.internal.runtime.READ_WRITE
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.runtime.WRITE
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.ExecutionPlanDescription
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.api.query.CompilerInfo
import org.neo4j.kernel.api.query.DeprecationNotificationsProvider
import org.neo4j.kernel.api.query.LookupIndexUsage
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.kernel.api.query.RelationshipTypeIndexUsage
import org.neo4j.kernel.api.query.SchemaIndexUsage
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.impl.query.NotificationConfiguration
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.monitoring.Monitors
import org.neo4j.notifications.NotificationImplementation
import org.neo4j.notifications.NotificationWrapping.asKernelNotification
import org.neo4j.values.virtual.MapValue

import java.util.function.Supplier

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Composite [[Compiler]], which uses a [[CypherPlanner]] and [[CypherRuntime]] to compile
 * a query into a [[ExecutableQuery]].
 *
 * @param planner the planner
 * @param runtime the runtime
 * @param contextManager the runtime context manager
 * @param kernelMonitors monitors support
 * @tparam CONTEXT type of runtime context used
 */
case class CypherCurrentCompiler[CONTEXT <: RuntimeContext](
  planner: CypherPlanner,
  runtime: CypherRuntime[CONTEXT],
  contextManager: RuntimeContextManager[CONTEXT],
  kernelMonitors: Monitors,
  queryCaches: CypherQueryCaches
) extends org.neo4j.cypher.internal.Compiler {

  /**
   * Compile [[InputQuery]] into [[ExecutableQuery]].
   *
   * @param query                   query to convert
   * @param tracer                  compilation tracer to which events of the compilation process are reported
   * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
   * @throws org.neo4j.exceptions.Neo4jException public cypher exceptions on compilation problems
   * @return a compiled and executable query
   */
  override def compile(
    query: InputQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    notificationLogger: InternalNotificationLogger,
    sessionDatabase: DatabaseReference
  ): ExecutableQuery = {

    // we only pass in the runtime to be able to support checking against the correct CommandManagementRuntime
    val logicalPlanResult = query match {
      case fullyParsedQuery: FullyParsedQuery =>
        planner.plan(fullyParsedQuery, tracer, transactionalContext, params, runtime, notificationLogger)
      case preParsedQuery: PreParsedQuery =>
        planner.parseAndPlan(
          preParsedQuery,
          tracer,
          transactionalContext,
          params,
          runtime,
          notificationLogger,
          sessionDatabase
        )
    }

    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      logicalPlanResult.logicalPlanState.planningAttributes.hasEqualSizeAttributes,
      "All planning attributes should contain the same plans"
    )

    val planState = logicalPlanResult.logicalPlanState
    val logicalPlan = planState.logicalPlan
    val queryType = getQueryType(planState)
    val cachedExecutionPlan =
      queryCaches.executionPlanCache.computeIfAbsent(
        cacheWhen = logicalPlanResult.shouldBeCached,
        key = ExecutionPlanCacheKey(
          query.options.executionPlanCacheKey,
          logicalPlan,
          planState.planningAttributes.cacheKey
        ),
        compute =
          computeExecutionPlan(query, transactionalContext, logicalPlanResult, planState, logicalPlan, queryType)
      )

    new CypherExecutableQuery(
      logicalPlan,
      queryType == READ_ONLY || queryType == DBMS_READ,
      cachedExecutionPlan.effectiveCardinalities,
      logicalPlanResult.plannerContext.debugOptions.rawCardinalitiesEnabled,
      logicalPlanResult.plannerContext.debugOptions.renderDistinctnessEnabled,
      cachedExecutionPlan.providedOrders,
      cachedExecutionPlan.executionPlan,
      (logicalPlanResult.notifications ++ query.notifications).distinct,
      logicalPlanResult.reusability,
      logicalPlanResult.paramNames.toArray,
      logicalPlanResult.extractedParams,
      buildCompilerInfo(logicalPlan, planState.plannerName, cachedExecutionPlan.executionPlan.runtimeName),
      planState.plannerName,
      queryType,
      logicalPlanResult.shouldBeCached,
      contextManager.config.enableMonitors,
      logicalPlanResult.queryObfuscator,
      contextManager.config.renderPlanDescription,
      kernelMonitors,
      query.options.queryOptions.cypherVersion.actualVersion
    )
  }

  private def computeExecutionPlan(
    query: InputQuery,
    transactionalContext: TransactionalContext,
    logicalPlanResult: LogicalPlanResult,
    planState: CachableLogicalPlanState,
    logicalPlan: LogicalPlan,
    queryType: InternalQueryType
  ): CachedExecutionPlan = {
    val runtimeContext = contextManager.create(
      query.options.queryOptions.cypherVersion.actualVersion,
      logicalPlanResult.plannerContext.planContext,
      transactionalContext.kernelTransaction().schemaRead(),
      transactionalContext.kernelTransaction().procedures(),
      logicalPlanResult.plannerContext.clock,
      logicalPlanResult.plannerContext.debugOptions,
      query.options.useCompiledExpressions,
      query.options.materializedEntitiesMode,
      query.options.queryOptions.operatorEngine,
      query.options.queryOptions.interpretedPipesFallback,
      planState.anonymousVariableNameGenerator,
      transactionalContext.kernelTransaction()
    )

    val planningAttributesCopy = planState.planningAttributes
      // Make copy, so per-runtime logical plan rewriting does not mutate cached attributes.
      .createCopy()
      // Reduce to PlanningAttributesCacheKey so that we (by type checking) know that all attributes needed for
      // computing an execution plan is also part of the cache key.
      .cacheKey

    val logicalQuery = LogicalQuery(
      logicalPlan,
      planState.queryText,
      queryType == READ_ONLY || queryType == DBMS_READ,
      planState.returnColumns.toArray,
      planState.semanticTable,
      planningAttributesCopy.effectiveCardinalities,
      planningAttributesCopy.providedOrders,
      planningAttributesCopy.leveragedOrders,
      planState.hasLoadCSV,
      new SequentialIdGen(planningAttributesCopy.cardinalities.size),
      query.options.queryOptions.executionMode == CypherExecutionMode.profile
    )

    try {
      CachedExecutionPlan(
        executionPlan = runtime.compileToExecutable(logicalQuery, runtimeContext),
        effectiveCardinalities = planningAttributesCopy.effectiveCardinalities,
        providedOrders = planningAttributesCopy.providedOrders
      )
    } catch {
      case e: Exception =>
        // The logical plan is valuable information if we fail to create an executionPlan
        val lpStr = LogicalPlanToPlanBuilderString(logicalPlan)
        val planInfo = new InternalException("Failed with plan:\n" + lpStr)
        e.addSuppressed(planInfo)
        throw e
    }
  }

  private def buildCompilerInfo(logicalPlan: LogicalPlan, plannerName: PlannerName, runtimeName: RuntimeName) = {
    val schemaIndexUsage = ListBuffer.empty[SchemaIndexUsage]
    val relationshipTypeIndexUsage = ListBuffer.empty[RelationshipTypeIndexUsage]
    val lookupIndexUsage = ListBuffer.empty[LookupIndexUsage]

    logicalPlan.indexUsage().foreach {
      case SchemaLabelIndexUsage(identifier, labelId, label, propertyKeys) =>
        schemaIndexUsage.addOne(new SchemaIndexUsage(
          identifier.name,
          labelId,
          label,
          propertyKeys.map(_.nameId.id).toArray,
          propertyKeys.map(_.name): _*
        ))

      case SchemaRelationshipIndexUsage(identifier, relTypeId, relType, propertyKeys) =>
        relationshipTypeIndexUsage.addOne(new RelationshipTypeIndexUsage(
          identifier.name,
          relTypeId,
          relType,
          propertyKeys.map(_.nameId.id).toArray,
          propertyKeys.map(_.name).toArray
        ))

      case SchemaIndexLookupUsage(identifier, entityType) =>
        lookupIndexUsage.addOne(new LookupIndexUsage(identifier.name, entityType))
    }

    new CompilerInfo(
      plannerName.name,
      runtimeName.name,
      schemaIndexUsage.asJava,
      relationshipTypeIndexUsage.asJava,
      lookupIndexUsage.asJava
    )
  }

  private def getQueryType(planState: CachableLogicalPlanState): InternalQueryType = {
    // check system and procedure runtimes first, because if this is true solveds will be empty
    runtime match {
      case m: AdministrationCommandRuntime if m.isApplicableAdministrationCommand(planState.logicalPlan) =>
        DBMS
      case _ =>
        val procedureOrSchema = SchemaCommandRuntime.queryType(planState.logicalPlan)
        if (procedureOrSchema.isDefined) {
          procedureOrSchema.get
        } else if (planHasDBMSProcedure(planState.logicalPlan)) {
          if (planState.planningAttributes.solveds(planState.logicalPlan.id).readOnly) {
            DBMS_READ
          } else {
            DBMS
          }
        } else if (planState.planningAttributes.solveds(planState.logicalPlan.id).readOnly) {
          READ_ONLY
        } else if (CypherCurrentCompiler.columnNames(planState.logicalPlan).isEmpty) {
          WRITE
        } else {
          READ_WRITE
        }
    }
  }

  private def planHasDBMSProcedure(logicalPlan: LogicalPlan): Boolean =
    logicalPlan.folder.treeExists {
      case procCall: ProcedureCall if procCall.call.signature.accessMode == ProcedureDbmsAccess => true
    }

  /**
   * Clear the caches of this caching compiler.
   *
   * @return the number of entries that were cleared in any cache
   */
  def clearCaches(): Long = {
    // TODO: global clear on queryCaches?
    Math.max(planner.clearCaches(), queryCaches.executionPlanCache.clear())
  }

  def clearExecutionPlanCache(): Unit = queryCaches.executionPlanCache.clear()

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit =
    planner.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
}

object CypherCurrentCompiler {

  private def getTerminationStatus(error: Throwable): Status = {
    error match {
      case e: HasStatus =>
        e.status()
      case _ =>
        Status.Transaction.QueryExecutionFailedOnTransaction
    }
  }

  private def columnNames(logicalPlan: LogicalPlan): Array[String] =
    logicalPlan match {
      case produceResult: ProduceResult => produceResult.columns.map(_.name).toArray

      case _ => Array()
    }

  private[internal] class CypherExecutableQuery(
    logicalPlan: LogicalPlan,
    readOnly: Boolean,
    effectiveCardinalities: EffectiveCardinalities,
    rawCardinalitiesInPlanDescription: Boolean,
    distinctnessInPlanDescription: Boolean,
    providedOrders: ProvidedOrders,
    executionPlan: ExecutionPlan,
    planningNotifications: IndexedSeq[InternalNotification],
    reusabilityState: ReusabilityState,
    override val paramNames: Array[String],
    override val extractedParams: MapValue,
    override val compilerInfo: CompilerInfo,
    plannerName: PlannerName,
    internalQueryType: InternalQueryType,
    override val shouldBeCached: Boolean,
    enableMonitors: Boolean,
    override val queryObfuscator: QueryObfuscator,
    renderPlanDescription: Boolean,
    kernelMonitors: Monitors,
    cypherVersion: CypherVersion
  ) extends ExecutableQuery {

    // Monitors are implemented via dynamic proxies which are slow compared to NOOP which is why we want to able to completely disable
    private val searchMonitor =
      if (enableMonitors) kernelMonitors.newMonitor(classOf[IndexSearchMonitor]) else IndexSearchMonitor.NOOP

    private val resourceMonitor =
      if (enableMonitors) kernelMonitors.newMonitor(classOf[ResourceMonitor]) else ResourceMonitor.NOOP

    private val planDescriptionBuilder =
      PlanDescriptionBuilder(
        executionPlan.rewrittenPlan.getOrElse(logicalPlan),
        plannerName,
        readOnly,
        effectiveCardinalities,
        rawCardinalitiesInPlanDescription,
        distinctnessInPlanDescription,
        providedOrders,
        executionPlan,
        renderPlanDescription,
        cypherVersion
      )

    private def createQueryContext(transactionalContext: TransactionalContext, taskCloser: TaskCloser) = {
      val resourceManager = executionPlan.threadSafeExecutionResources() match {
        case Some(resourceManagerFactory) => resourceManagerFactory(resourceMonitor)
        case None =>
          new ResourceManager(resourceMonitor, transactionalContext.kernelTransaction().memoryTracker())
      }
      val txContextWrapper = TransactionalContextWrapper(transactionalContext)
      val statement = transactionalContext.statement()
      statement.registerCloseableResource(resourceManager)
      taskCloser.addTask(_ => statement.unregisterCloseableResource(resourceManager))

      val ctx = new TransactionBoundQueryContext(txContextWrapper, resourceManager)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    override def notifications: IndexedSeq[InternalNotification] = planningNotifications

    override def execute(
      transactionalContext: TransactionalContext,
      isOutermostQuery: Boolean,
      queryOptions: QueryOptions,
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      queryMonitor: QueryExecutionMonitor,
      subscriber: QuerySubscriber
    ): QueryExecution = {

      val taskCloser = new TaskCloser
      val queryContext = createQueryContext(transactionalContext, taskCloser)
      val exceptionTranslatingContext = queryContext.transactionalContext
      val outerCloseable: AutoCloseable =
        if (isOutermostQuery) {
          taskCloser.addTask {
            case Failure =>
              val status = Failure.status
              exceptionTranslatingContext.markForTermination(status)
            case Error(e) =>
              val status = getTerminationStatus(e)
              exceptionTranslatingContext.markForTermination(status)
            case _ =>
          }
          () => {
            exceptionTranslatingContext.close()
            // NOTE: We leave it up to outer layers to rollback on failure.
            // This will only close the statement.
          }
        } else {
          NoOuterCloseable
        }

      taskCloser.addTask(_ => queryContext.resources.close())
      try {
        innerExecute(
          transactionalContext,
          queryOptions,
          taskCloser,
          outerCloseable,
          queryContext,
          params,
          prePopulateResults,
          input,
          queryMonitor,
          subscriber,
          isOutermostQuery
        )
      } catch {
        case e: Throwable =>
          QuerySubscriber.safelyOnError(subscriber, e)
          taskCloser.close(Error(e))
          // NOTE: We leave it up to outer layers to rollback on failure
          outerCloseable.close()
          new FailedExecutionResult(columnNames(logicalPlan), internalQueryType, subscriber)
      }
    }

    private def innerExecute(
      transactionalContext: TransactionalContext,
      queryOptions: QueryOptions,
      taskCloser: TaskCloser,
      outerCloseable: AutoCloseable,
      queryContext: QueryContext,
      params: MapValue,
      prePopulateResults: Boolean,
      input: InputDataStream,
      queryMonitor: QueryExecutionMonitor,
      subscriber: QuerySubscriber,
      isOutermostQuery: Boolean
    ): InternalExecutionResult = {

      val innerExecutionMode = queryOptions.queryOptions.executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.default => NormalMode
      }

      val monitor = if (isOutermostQuery) queryMonitor else QueryExecutionMonitor.NO_OP
      monitor.startExecution(transactionalContext.executingQuery())

      val notificationConfig =
        transactionalContext.queryExecutingConfiguration().notificationFilters()
      val filteredPlannerNotifications: Seq[NotificationImplementation] =
        if (notificationConfig eq NotificationConfiguration.NONE) {
          Seq.empty[NotificationImplementation]
        } else {
          (planningNotifications ++ executionPlan.notifications)
            .map(asKernelNotification(Some(queryOptions.offset)))
            .filter(notificationConfig.includes(_))
        }

      val inner =
        if (innerExecutionMode == ExplainMode) {
          taskCloser.close(Success)
          outerCloseable.close()
          val columns = columnNames(logicalPlan)

          new ExplainExecutionResult(
            columns,
            planDescriptionBuilder.explain(),
            internalQueryType,
            filteredPlannerNotifications.toSet,
            subscriber
          )
        } else {

          val runtimeResult: RuntimeResult =
            executionPlan.run(queryContext, innerExecutionMode, params, prePopulateResults, input, subscriber)

          val filteredRuntimeNotifications = runtimeResult.notifications().asScala
            .map(asKernelNotification(None))
            .filter(filterNotifications(_, notificationConfig))
          if (isOutermostQuery) {
            transactionalContext.executingQuery().onExecutionStarted(runtimeResult)
          }
          taskCloser.addTask(_ => runtimeResult.close())

          new StandardInternalExecutionResult(
            runtimeResult,
            taskCloser,
            outerCloseable,
            internalQueryType,
            innerExecutionMode,
            planDescriptionBuilder,
            subscriber,
            filteredPlannerNotifications ++ filteredRuntimeNotifications
          )
        }

      ClosingExecutionResult.wrapAndInitiate(
        transactionalContext.executingQuery(),
        inner,
        monitor,
        subscriber
      )
    }

    private def filterNotifications(
      notification: NotificationImplementation,
      notificationConfig: NotificationConfiguration
    ): Boolean =
      notificationConfig.includes(notification)

    override def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState =
      reusabilityState

    override def planDescriptionSupplier(): Supplier[ExecutionPlanDescription] = {
      val builder = planDescriptionBuilder
      () => builder.explain()
    }

    override def deprecationNotificationsProvider(queryOptionsOffset: InputPosition)
      : DeprecationNotificationsProvider = {
      CypherDeprecationNotificationsProvider(
        queryOptionsOffset = queryOptionsOffset,
        notifications = executionPlan.notifications.view ++ planningNotifications.view
      )
    }
  }
}
