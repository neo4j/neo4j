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
package org.neo4j.cypher.internal.planning

import java.time.Clock
import java.util.function.BiFunction

import org.neo4j.cypher._
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler._
import org.neo4j.cypher.internal.compiler.phases.{LogicalPlanState, PlannerContext, PlannerContextCreator}
import org.neo4j.cypher.internal.compiler.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory, simpleExpressionEvaluator}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName, PlanContext}
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.{ExceptionTranslatingPlanContext, TransactionBoundPlanContext}
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{GeneratingNamer, InnerVariableNamer}
import org.neo4j.cypher.internal.v4_0.util.{InputPosition, InternalNotification}
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.{compiler, _}
import org.neo4j.exceptions.{DatabaseAdministrationException, Neo4jException, SyntaxException}
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.kernel.impl.api.SchemaStateKey
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.Log
import org.neo4j.monitoring
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

object CypherPlanner {
  /**
    * This back-door is intended for quick handling of bugs and support cases
    * where we need to inject some specific indexes and statistics.
    */
  var customPlanContextCreator: Option[(TransactionalContextWrapper, InternalNotificationLogger) => PlanContext] = None
}

/**
  * Cypher planner, which either parses and plans a [[PreParsedQuery]] into a [[LogicalPlanResult]] or just plans [[FullyParsedQuery]].
  */
case class CypherPlanner(config: CypherPlannerConfiguration,
                         clock: Clock,
                         kernelMonitors: monitoring.Monitors,
                         log: Log,
                         plannerOption: CypherPlannerOption,
                         updateStrategy: CypherUpdateStrategy,
                         txIdProvider: () => Long,
                         compatibilityMode: Boolean) {

  private val parsedQueries = new LFUCache[String, BaseState](config.queryCacheSize)

  private val monitors: Monitors = WrappedMonitors(kernelMonitors)

  private val cacheTracer: CacheTracer[Pair[Statement, ParameterTypeMap]] = monitors.newMonitor[CacheTracer[Pair[Statement, ParameterTypeMap]]]("cypher")

  private val planCache: AstLogicalPlanCache[Statement] =
    new AstLogicalPlanCache(config.queryCacheSize,
      cacheTracer,
      clock,
      config.statsDivergenceCalculator,
      txIdProvider)

  monitors.addMonitorListener(planCache.logStalePlanRemovalMonitor(log), "cypher")

  private val contextCreator: PlannerContextCreator.type = PlannerContextCreator

  private val maybeUpdateStrategy: Option[UpdateStrategy] = updateStrategy match {
    case CypherUpdateStrategy.eager => Some(eagerUpdateStrategy)
    case _ => None
  }

  private val rewriterSequencer: String => RewriterStepSequencer = {
    import Assertion._
    import RewriterStepSequencer._

    if (assertionsEnabled()) newValidating else newPlain
  }

  private val plannerName: CostBasedPlannerName =
    plannerOption match {
      case CypherPlannerOption.default => CostBasedPlannerName.default
      case CypherPlannerOption.cost | CypherPlannerOption.idp => IDPPlannerName
      case CypherPlannerOption.dp => DPPlannerName
      case _ => throw new IllegalArgumentException(s"unknown cost based planner: ${plannerOption.name}")
    }

  private val planner: compiler.CypherPlanner[PlannerContext] =
    new CypherPlannerFactory().costBasedCompiler(config, clock, monitors, rewriterSequencer,
      maybeUpdateStrategy, contextCreator)

  private val schemaStateKey: SchemaStateKey = SchemaStateKey.newKey()

  /**
    * Clear the caches of this caching compiler.
    *
    * @return the number of entries that were cleared
    */
  def clearCaches(): Long = {
    Math.max(parsedQueries.clear(), planCache.clear())
  }

  /**
    * Get the parsed query from cache, or parses and caches it.
    */
  @throws(classOf[SyntaxException])
  private def getOrParse(preParsedQuery: PreParsedQuery,
                           params: MapValue,
                           notificationLogger: InternalNotificationLogger,
                           offset: InputPosition,
                           tracer: CompilationPhaseTracer,
                           innerVariableNamer: InnerVariableNamer,
                          ): BaseState = {
    parsedQueries.get(preParsedQuery.statementWithVersionAndPlanner).getOrElse {
      val parsedQuery = planner.parseQuery(preParsedQuery.statement,
        preParsedQuery.rawStatement,
        notificationLogger,
        preParsedQuery.options.planner.name,
        preParsedQuery.options.debugOptions,
        Some(offset),
        tracer,
        innerVariableNamer,
        params,
        compatibilityMode)
      parsedQueries.put(preParsedQuery.statementWithVersionAndPlanner, parsedQuery)
      parsedQuery
    }
  }

  /**
    * Compile pre-parsed query into a logical plan.
    *
    * @param preParsedQuery       pre-parsed query to convert
    * @param tracer               tracer to which events of the parsing and planning are reported
    * @param transactionalContext transactional context to use during parsing and planning
    * @throws Neo4jException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  def parseAndPlan(preParsedQuery: PreParsedQuery,
                   tracer: CompilationPhaseTracer,
                   transactionalContext: TransactionalContext,
                   params: MapValue,
                   runtime: CypherRuntime[_]
                  ): LogicalPlanResult = {
    val notificationLogger = new RecordingNotificationLogger(Some(preParsedQuery.options.offset))
    val innerVariableNamer = new GeneratingNamer

    val syntacticQuery = getOrParse(preParsedQuery, params, notificationLogger, preParsedQuery.options.offset, tracer, innerVariableNamer)

    // The parser populates the notificationLogger as a side-effect of its work, therefore
    // in the case of a cached query the notificationLogger will not be properly filled
    syntacticQuery.maybeSemantics.map(_.notifications).getOrElse(Set.empty).foreach(notificationLogger.log)

    doPlan(syntacticQuery, preParsedQuery.options, tracer, transactionalContext, params, runtime, notificationLogger, innerVariableNamer,
      preParsedQuery.rawStatement)
  }

  /**
    * Plan fully-parsed query into a logical plan.
    *
    * @param fullyParsedQuery     a fully-parsed query to plan
    * @param tracer               tracer to which events of the parsing and planning are reported
    * @param transactionalContext transactional context to use during parsing and planning
    * @throws Neo4jException public cypher exceptions on compilation problems
    * @return a logical plan result
    */
  @throws[Neo4jException]
  def plan(fullyParsedQuery: FullyParsedQuery,
           tracer: CompilationPhaseTracer,
           transactionalContext: TransactionalContext,
           params: MapValue,
           runtime: CypherRuntime[_]
          ): LogicalPlanResult = {
    val notificationLogger = new RecordingNotificationLogger(Some(fullyParsedQuery.options.offset))
    doPlan(fullyParsedQuery.state, fullyParsedQuery.options, tracer, transactionalContext, params, runtime, notificationLogger, new GeneratingNamer,
      fullyParsedQuery.state.queryText)
  }

  private def doPlan(syntacticQuery: BaseState,
                     options: QueryOptions,
                     tracer: CompilationPhaseTracer,
                     transactionalContext: TransactionalContext,
                     params: MapValue,
                     runtime: CypherRuntime[_],
                     notificationLogger: InternalNotificationLogger,
                     innerVariableNamer: InnerVariableNamer,
                     rawQueryText: String
                    ): LogicalPlanResult = {
    val transactionalContextWrapper = TransactionalContextWrapper(transactionalContext)
    // Context used for db communication during planning
    val createPlanContext = CypherPlanner.customPlanContextCreator.getOrElse(TransactionBoundPlanContext.apply _)
    val planContext = new ExceptionTranslatingPlanContext(createPlanContext(transactionalContextWrapper, notificationLogger))

    // Context used to create logical plans
    val logicalPlanIdGen = new SequentialIdGen()
    val context = contextCreator.create(tracer,
      notificationLogger,
      planContext,
      rawQueryText,
      options.debugOptions,
      Some(options.offset),
      monitors,
      CachedMetricsFactory(SimpleMetricsFactory),
      createQueryGraphSolver(),
      config,
      maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
      clock,
      logicalPlanIdGen,
      simpleExpressionEvaluator,
      innerVariableNamer,
      params)

    // Prepare query for caching
    val preparedQuery = planner.normalizeQuery(syntacticQuery, context)
    val queryParamNames: Seq[String] = preparedQuery.statement().findByAllClass[Parameter].map(x => x.name).distinct

    checkForSchemaChanges(transactionalContextWrapper)

    // If the query is not cached we want to do the full planning
    def createPlan(shouldBeCached: Boolean, missingParameterNames: Seq[String] = Seq.empty): CacheableLogicalPlan = {
      var shouldCache = shouldBeCached
      val logicalPlanStateOld = planner.planPreparedQuery(preparedQuery, context)
      val hasLoadCsv = logicalPlanStateOld.logicalPlan.treeFind[LogicalPlan] {
        case _: LoadCSV => true
      }.nonEmpty
      val logicalPlanState = logicalPlanStateOld.copy(hasLoadCSV = hasLoadCsv)
      notification.LogicalPlanNotifications
        .checkForNotifications(logicalPlanState.maybeLogicalPlan.get, planContext, config)
        .foreach(notificationLogger.log)
      if (missingParameterNames.nonEmpty) {
        notificationLogger.log(MissingParametersNotification(missingParameterNames))
      }
      val reusabilityState = runtime match {
        case m: AdministrationCommandRuntime =>
          if (m.isApplicableAdministrationCommand(logicalPlanState)) {
            shouldCache = false
            FineToReuse
          } else {
            logicalPlanState.maybeLogicalPlan match {
              case Some(ProcedureCall(_, ResolvedCall(signature, _, _, _, _))) if signature.systemProcedure => {
                shouldCache = false
                FineToReuse
              }
              case Some(_: ProcedureCall) => throw new DatabaseAdministrationException("Attempting invalid procedure call in administration runtime")
              case Some(plan: MultiDatabaseLogicalPlan) => throw plan.invalid("Unsupported administration command: " + logicalPlanState.queryText)
              case _ => throw new DatabaseAdministrationException("Attempting invalid administration command in administration runtime")
            }
          }
        case _ if SchemaCommandRuntime.isApplicable(logicalPlanState) => FineToReuse
        case _ =>
          val fingerprint = PlanFingerprint.take(clock, planContext.txIdProvider, planContext.statistics)
          val fingerprintReference = new PlanFingerprintReference(fingerprint)
          MaybeReusable(fingerprintReference)
      }
      CacheableLogicalPlan(logicalPlanState, reusabilityState, notificationLogger.notifications, shouldCache)
    }

    val autoExtractParams = ValueConversion.asValues(preparedQuery.extractedParams()) // only extracted ones
    // Filter the parameters to retain only those that are actually used in the query (or a subset of them, if not enough
    // parameters where given in the first place)
    val filteredParams: MapValue = params.updatedWith(autoExtractParams).filter(new BiFunction[String, AnyValue, java.lang.Boolean] {
      override def apply(name: String, value: AnyValue): java.lang.Boolean = queryParamNames.contains(name)
    })

    val enoughParametersSupplied = queryParamNames.size == filteredParams.size // this is relevant if the query has parameters

    val cacheableLogicalPlan =
    // We don't want to cache any query without enough given parameters (although EXPLAIN queries will succeed)
      if (options.debugOptions.isEmpty && (queryParamNames.isEmpty || enoughParametersSupplied)) {
        planCache.computeIfAbsentOrStale(Pair.of(syntacticQuery.statement(), QueryCache.extractParameterTypeMap(filteredParams)),
          transactionalContext,
          () => createPlan(shouldBeCached = true),
          _ => None,
          syntacticQuery.queryText).executableQuery
      } else if (!enoughParametersSupplied) {
        createPlan(shouldBeCached = false, missingParameterNames = queryParamNames.filterNot(filteredParams.containsKey))
      } else {
        createPlan(shouldBeCached = false)
      }
    LogicalPlanResult(
      cacheableLogicalPlan.logicalPlanState,
      queryParamNames,
      autoExtractParams,
      cacheableLogicalPlan.reusability,
      context,
      cacheableLogicalPlan.notifications,
      cacheableLogicalPlan.shouldBeCached)
  }

  private def checkForSchemaChanges(tcw: TransactionalContextWrapper): Unit =
    tcw.getOrCreateFromSchemaState(schemaStateKey, planCache.clear())

  private def createQueryGraphSolver(): IDPQueryGraphSolver =
    plannerName match {
      case IDPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val solverConfig = new ConfigurableIDPSolverConfig(
          maxTableSize = config.idpMaxTableSize,
          iterationDurationLimit = config.idpIterationDuration
        )
        val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

      case DPPlannerName =>
        val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
        val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
        IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
    }
}

case class LogicalPlanResult(logicalPlanState: LogicalPlanState,
                             paramNames: Seq[String],
                             extractedParams: MapValue,
                             reusability: ReusabilityState,
                             plannerContext: PlannerContext,
                             notifications: Set[InternalNotification],
                             shouldBeCached: Boolean)

trait CypherCacheFlushingMonitor {
  def cacheFlushDetected(sizeBeforeFlush: Long) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}

  def cacheMiss(key: T) {}

  def cacheDiscard(key: T, userKey: String, secondsSinceReplan: Int) {}

  def cacheRecompile(key: T) {}
}

trait CypherCacheMonitor[T] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor
