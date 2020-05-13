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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.NotificationWrapping.asKernelNotification
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.plandescription.{InternalPlanDescription, PlanDescriptionBuilder}
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.{Cardinalities, ProvidedOrders}
import org.neo4j.cypher.internal.planning._
import org.neo4j.cypher.internal.result.{ClosingExecutionResult, ExplainExecutionResult, StandardInternalExecutionResult, _}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.runtime.{ExecutableQuery => _, _}
import org.neo4j.cypher.internal.v4_0.expressions.Parameter
import org.neo4j.cypher.internal.v4_0.frontend.PlannerName
import org.neo4j.cypher.internal.v4_0.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v4_0.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.v4_0.util.{InternalNotification, TaskCloser}
import org.neo4j.cypher.{CypherExecutionMode, CypherVersion}
import org.neo4j.exceptions.{Neo4jException, ParameterNotFoundException, ParameterWrongTypeException}
import org.neo4j.graphdb.{Notification, QueryExecutionType}
import org.neo4j.kernel.api.query.{CompilerInfo, SchemaIndexUsage}
import org.neo4j.kernel.impl.query.{QueryExecution, QueryExecutionMonitor, QuerySubscriber, TransactionalContext}
import org.neo4j.monitoring.Monitors
import org.neo4j.string.UTF8
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

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
case class CypherCurrentCompiler[CONTEXT <: RuntimeContext](planner: CypherPlanner,
                                                            runtime: CypherRuntime[CONTEXT],
                                                            contextManager: RuntimeContextManager[CONTEXT],
                                                            kernelMonitors: Monitors
                                                           ) extends org.neo4j.cypher.internal.Compiler {

  /**
    * Compile [[InputQuery]] into [[ExecutableQuery]].
    *
    * @param query                   query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
    * @throws Neo4jException public cypher exceptions on compilation problems
    * @return a compiled and executable query
    */
  override def compile(query: InputQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       transactionalContext: TransactionalContext,
                       params: MapValue
                      ): ExecutableQuery = {

    def resolveParameterForManagementCommands(logicalPlan: LogicalPlan): LogicalPlan = {
      def getParamValue(paramPassword: Parameter) = {
        params.get(paramPassword.name) match {
          case param: TextValue => UTF8.encode(param.stringValue())
          case IsNoValue() => throw new ParameterNotFoundException("Expected parameter(s): " + paramPassword.name)
          case param => throw new ParameterWrongTypeException("Only string values are accepted as password, got: " + param.getTypeName)
        }
      }

      logicalPlan match {
        case l@LogSystemCommand(source: MultiDatabaseLogicalPlan, _) =>
          LogSystemCommand(resolveParameterForManagementCommands(source).asInstanceOf[MultiDatabaseLogicalPlan], l.command)(new SequentialIdGen(l.id.x + 1))
        case c@CreateUser(_, _, _, Some(paramPassword), _, _) =>
          CreateUser(c.source, c.userName, Some(getParamValue(paramPassword)), None, c.requirePasswordChange, c.suspended)(new SequentialIdGen(c.id.x + 1))
        case a@AlterUser(_, _, _, Some(paramPassword), _, _) =>
          AlterUser(a.source, a.userName, Some(getParamValue(paramPassword)), None, a.requirePasswordChange, a.suspended)(new SequentialIdGen(a.id.x + 1))
        case p@SetOwnPassword(_, Some(newParamPassword), _, None) =>
          SetOwnPassword(Some(getParamValue(newParamPassword)), None, p.currentStringPassword, None)(new SequentialIdGen(p.id.x + 1))
        case p@SetOwnPassword(_, None, _, Some(currentParamPassword)) =>
          SetOwnPassword(p.newStringPassword, None, Some(getParamValue(currentParamPassword)), None)(new SequentialIdGen(p.id.x + 1))
        case p@SetOwnPassword(_, Some(newParamPassword), _, Some(currentParamPassword)) =>
          SetOwnPassword(Some(getParamValue(newParamPassword)), None, Some(getParamValue(currentParamPassword)), None)(new SequentialIdGen(p.id.x + 1))
        case _ => // Not an administration command that needs resolving, do nothing
          logicalPlan
      }
    }

    // we only pass in the runtime to be able to support checking against the correct CommandManagementRuntime
    val logicalPlanResult = query match {
      case fullyParsedQuery: FullyParsedQuery => planner.plan(fullyParsedQuery, tracer, transactionalContext, params, runtime)
      case preParsedQuery: PreParsedQuery => planner.parseAndPlan(preParsedQuery, tracer, transactionalContext, params, runtime)
    }

    val planState = logicalPlanResult.logicalPlanState
    val logicalPlan: LogicalPlan = resolveParameterForManagementCommands(planState.logicalPlan)
    val queryType = getQueryType(planState)

    val runtimeContext = contextManager.create(logicalPlanResult.plannerContext.planContext,
                                               transactionalContext.kernelTransaction().schemaRead(),
                                               logicalPlanResult.plannerContext.clock,
                                               logicalPlanResult.plannerContext.debugOptions,
                                               query.options.useCompiledExpressions,
                                               query.options.materializedEntitiesMode,
                                               query.options.operatorEngine,
                                               query.options.interpretedPipesFallback)

    val logicalQuery = LogicalQuery(logicalPlan,
                                    planState.queryText,
                                    queryType == READ_ONLY,
                                    planState.returnColumns().toArray,
                                    planState.semanticTable(),
                                    planState.planningAttributes.cardinalities,
                                    planState.planningAttributes.providedOrders,
                                    planState.hasLoadCSV,
                                    planState.maybePeriodicCommit.flatMap(_.map(x => PeriodicCommitInfo(x.batchSize))))

    val securityContext = transactionalContext.securityContext()
    val executionPlan: ExecutionPlan = runtime.compileToExecutable(logicalQuery, runtimeContext, securityContext)

    new CypherExecutableQuery(
      logicalPlan,
      logicalQuery.readOnly,
      logicalPlanResult.logicalPlanState.planningAttributes.cardinalities,
      logicalPlanResult.logicalPlanState.planningAttributes.providedOrders,
      executionPlan,
      preParsingNotifications,
      logicalPlanResult.notifications,
      logicalPlanResult.reusability,
      logicalPlanResult.paramNames.toArray,
      logicalPlanResult.extractedParams,
      buildCompilerInfo(logicalPlan, planState.plannerName, executionPlan.runtimeName),
      planState.plannerName,
      query.options.version,
      queryType,
      logicalPlanResult.shouldBeCached,
      runtimeContext.config.enableMonitors)
  }

  private def buildCompilerInfo(logicalPlan: LogicalPlan,
                                plannerName: PlannerName,
                                runtimeName: RuntimeName): CompilerInfo =

    new CompilerInfo(plannerName.name, runtimeName.name, logicalPlan.indexUsage.map {
      case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
      case SchemaIndexScanUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
    }.asJava)

  private def getQueryType(planState: LogicalPlanState): InternalQueryType = {
    // check system and procedure runtimes first, because if this is true solveds will be empty
    runtime match {
      case m:AdministrationCommandRuntime if m.isApplicableAdministrationCommand(planState) =>
          DBMS
      case _ =>
        val procedureOrSchema = SchemaCommandRuntime.queryType(planState.logicalPlan)
        if (procedureOrSchema.isDefined)
          procedureOrSchema.get
        else if (planHasDBMSProcedure(planState.logicalPlan))
          DBMS
        else if (planState.planningAttributes.solveds(planState.logicalPlan.id).readOnly)
          READ_ONLY
        else if (columnNames(planState.logicalPlan).isEmpty)
          WRITE
        else
          READ_WRITE
    }
  }

  private def planHasDBMSProcedure(logicalPlan: LogicalPlan): Boolean =
    logicalPlan.treeExists {
      case procCall: ProcedureCall if procCall.call.signature.accessMode.isInstanceOf[ProcedureDbmsAccess] => true
    }

  private def columnNames(logicalPlan: LogicalPlan): Array[String] =
    logicalPlan match {
      case produceResult: ProduceResult => produceResult.columns.toArray

      case _ => Array()
    }

  protected class CypherExecutableQuery(logicalPlan: LogicalPlan,
                                        readOnly: Boolean,
                                        cardinalities: Cardinalities,
                                        providedOrders: ProvidedOrders,
                                        executionPlan: ExecutionPlan,
                                        preParsingNotifications: Set[Notification],
                                        planningNotifications: Set[InternalNotification],
                                        reusabilityState: ReusabilityState,
                                        override val paramNames: Array[String],
                                        override val extractedParams: MapValue,
                                        override val compilerInfo: CompilerInfo,
                                        plannerName: PlannerName,
                                        cypherVersion: CypherVersion,
                                        internalQueryType: InternalQueryType,
                                        override val shouldBeCached: Boolean,
                                        enableMonitors: Boolean) extends ExecutableQuery {

    //Monitors are implemented via dynamic proxies which are slow compared to NOOP which is why we want to able to completely disable
    private val searchMonitor = if (enableMonitors) kernelMonitors.newMonitor(classOf[IndexSearchMonitor]) else IndexSearchMonitor.NOOP
    private val resourceMonitor = if (enableMonitors) kernelMonitors.newMonitor(classOf[ResourceMonitor]) else ResourceMonitor.NOOP

    private val planDescriptionBuilder =
      new PlanDescriptionBuilder(logicalPlan,
        plannerName,
        cypherVersion,
        readOnly,
        cardinalities,
        providedOrders,
        executionPlan.runtimeName,
        executionPlan.metadata)

    private def getQueryContext(transactionalContext: TransactionalContext, debugOptions: Set[String], taskCloser: TaskCloser) = {
      val (threadSafeCursorFactory, resourceManager) = executionPlan.threadSafeExecutionResources() match {
        case Some((tFactory, rFactory)) => (tFactory, rFactory(resourceMonitor))
        case None => (null, new ResourceManager(resourceMonitor))
      }
      val txContextWrapper = TransactionalContextWrapper(transactionalContext, threadSafeCursorFactory)
      val statement = transactionalContext.statement()
      statement.registerCloseableResource(resourceManager)
      taskCloser.addTask(_ => statement.unregisterCloseableResource(resourceManager))

      val ctx = new TransactionBoundQueryContext(txContextWrapper, resourceManager)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    override def notifications: Set[InternalNotification] = planningNotifications

    override def execute(transactionalContext: TransactionalContext,
                         isOutermostQuery: Boolean,
                         queryOptions: QueryOptions,
                         params: MapValue,
                         prePopulateResults: Boolean,
                         input: InputDataStream,
                         subscriber: QuerySubscriber): QueryExecution = {

      val taskCloser = new TaskCloser
      val queryContext = getQueryContext(transactionalContext, queryOptions.debugOptions, taskCloser)
      if (isOutermostQuery) taskCloser.addTask(success => {
        val context = queryContext.transactionalContext
        if (!success) {
          context.rollback()
        } else {
          context.close()
        }
      })
      taskCloser.addTask(_ => queryContext.resources.close())
      try {
        innerExecute(transactionalContext, queryOptions, taskCloser, queryContext, params, prePopulateResults, input, subscriber, isOutermostQuery)
      } catch {
        case e: Throwable =>
          QuerySubscriber.safelyOnError(subscriber, e)
          taskCloser.close(false)
          transactionalContext.rollback()
          new FailedExecutionResult(columnNames(logicalPlan), internalQueryType, subscriber)
      }
    }

    private def innerExecute(transactionalContext: TransactionalContext,
                             queryOptions: QueryOptions,
                             taskCloser: TaskCloser,
                             queryContext: QueryContext,
                             params: MapValue,
                             prePopulateResults: Boolean,
                             input: InputDataStream,
                             subscriber: QuerySubscriber,
                             isOutermostQuery: Boolean): InternalExecutionResult = {

      val innerExecutionMode = queryOptions.executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }

      val inner = if (innerExecutionMode == ExplainMode) {
        taskCloser.close(success = true)
        val columns = columnNames(logicalPlan)

        val allNotifications =
          preParsingNotifications ++ (planningNotifications ++ executionPlan.notifications)
            .map(asKernelNotification(Some(queryOptions.offset)))
        new ExplainExecutionResult(columns,
                               planDescriptionBuilder.explain(),
                               internalQueryType, allNotifications, subscriber)
      } else {

        val runtimeResult = executionPlan.run(queryContext, innerExecutionMode, params, prePopulateResults, input, subscriber)

        if (isOutermostQuery)
          transactionalContext.executingQuery().onExecutionStarted(runtimeResult)

        taskCloser.addTask(_ => runtimeResult.close())

        new StandardInternalExecutionResult(queryContext,
                                            executionPlan.runtimeName,
                                            runtimeResult,
                                            taskCloser,
                                            internalQueryType,
                                            innerExecutionMode,
                                            planDescriptionBuilder,
                                            subscriber)
        }
      val monitor = if (isOutermostQuery) kernelMonitors.newMonitor(classOf[QueryExecutionMonitor]) else QueryExecutionMonitor.NO_OP
      ClosingExecutionResult.wrapAndInitiate(
        transactionalContext.executingQuery(),
        inner,
        monitor,
        subscriber
      )
    }

    override def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState = reusabilityState

    override def planDescription(): InternalPlanDescription = planDescriptionBuilder.explain()

    override def queryType: QueryExecutionType.QueryType = QueryTypeConversion.asPublic(internalQueryType)
  }

}
