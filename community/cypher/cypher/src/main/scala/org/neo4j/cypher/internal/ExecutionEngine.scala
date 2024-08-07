/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.StatsDivergenceCalculator
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.planning.CypherCacheMonitor
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.CompilationTracer.QueryCompilationEvent
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.LogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.lang
import java.time.Clock
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * See comment in MonitoringCacheTracer for justification of the existence of this type.
 */
trait ExecutionEngineQueryCacheMonitor extends CypherCacheMonitor[CacheKey[InputQuery.CacheKey]]

/**
 * This class constructs and initializes both the cypher compilers and runtimes, which are very expensive
 * operation. Please make sure this will be constructed only once and properly reused.
 */
class ExecutionEngine(val queryService: GraphDatabaseQueryService,
                      val kernelMonitors: Monitors,
                      val tracer: CompilationTracer,
                      val cacheTracer: CacheTracer[CacheKey[InputQuery.CacheKey]],
                      val config: CypherConfiguration,
                      val compilerLibrary: CompilerLibrary,
                      val cacheFactory: CaffeineCacheFactory,
                      val logProvider: LogProvider,
                      val clock: Clock = Clock.systemUTC() ) {

  require(queryService != null, "Can't work with a null graph database")

  // HELPER OBJECTS
  private val defaultQueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  private val preParser = new PreParser(config, cacheFactory)
  private val lastCommittedTxIdProvider = LastCommittedTxIdProvider(queryService)
  private def planReusabilitiy(executableQuery: ExecutableQuery,
                               transactionalContext: TransactionalContext): ReusabilityState =
    executableQuery.reusabilityState(lastCommittedTxIdProvider, transactionalContext)

  // Log on stale query discard from query cache
  private val log = logProvider.getLog( getClass )
  kernelMonitors.addMonitorListener( new ExecutionEngineQueryCacheMonitor {
    override def cacheDiscard(ignored: CacheKey[InputQuery.CacheKey], queryId: String, secondsSinceReplan: Int, maybeReason: Option[String]) {
      log.info(s"Discarded stale query from the query cache after $secondsSinceReplan seconds${maybeReason.fold("")(r => s". Reason: $r")}. Query id: $queryId")
    }
  })

  private val planStalenessCaller =
    new DefaultPlanStalenessCaller[ExecutableQuery](clock,
      StatsDivergenceCalculator.divergenceCalculatorFor(config.statsDivergenceCalculator),
      lastCommittedTxIdProvider,
      planReusabilitiy,
      log)

  private val queryCache: QueryCache[CacheKey[InputQuery.CacheKey], ExecutableQuery] =
    new QueryCache(cacheFactory, config.queryCacheSize, planStalenessCaller, cacheTracer)

  private val masterCompiler: MasterCompiler = new MasterCompiler(compilerLibrary)

  private val schemaHelper = new SchemaHelper(queryCache, masterCompiler)

  // ACTUAL FUNCTIONALITY

  /**
   * Executes query returns a `QueryExecution` that can be used to control demand to the provided `QuerySubscriber`.
   * This method assumes this is the only query running within the transaction, and therefor will register transaction closing
   * with the TaskCloser
   *
   * @param query the query to execute
   * @param params the parameters of the query
   * @param context the transactional context in which to run the query
   * @param profile if `true` run with profiling enabled
   * @param prePopulate if `true` pre populate all results
   * @param subscriber the subscriber where results will be streamed
   * @return a `QueryExecution` that controls the demand to the subscriber
   */
  def execute(query: String,
              params: MapValue,
              context: TransactionalContext,
              profile: Boolean,
              prePopulate: Boolean,
              subscriber: QuerySubscriber): QueryExecution = {
    defaultQueryExecutionMonitor.startProcessing(context.executingQuery())
    executeSubquery(query, params, context, isOutermostQuery = true, profile, prePopulate, subscriber)
  }

  /**
   * Executes query returns a `QueryExecution` that can be used to control demand to the provided `QuerySubscriber`
   * Note. This method will monitor the query start after it has been parsed. The caller is responsible for monitoring any query failing before this point.
   *
   * @param query       the query to execute
   * @param params      the parameters of the query
   * @param context     the context in which to run the query
   * @param prePopulate if `true` pre populate all results
   * @param subscriber  the subscriber where results will be streamed
   * @return a `QueryExecution` that controls the demand to the subscriber
   */
  def execute(query: FullyParsedQuery,
              params: MapValue,
              context: TransactionalContext,
              prePopulate: Boolean,
              input: InputDataStream,
              queryMonitor: QueryExecutionMonitor,
              subscriber: QuerySubscriber): QueryExecution = {
    queryMonitor.startProcessing(context.executingQuery())
    val queryTracer = tracer.compileQuery(query.description)
    closing(context, queryTracer) {
      doExecute(query, params, context, isOutermostQuery = true, prePopulate, input, queryMonitor, queryTracer, subscriber)
    }
  }

  /**
   * Executes query returns a `QueryExecution` that can be used to control demand to the provided `QuerySubscriber`.
   * This method assumes the query is running as one of many queries within a single transaction and therefor needs
   * to be told using the shouldCloseTransaction field if the TaskCloser needs to have a transaction close registered.
   *
   * @param query the query to execute
   * @param params the parameters of the query
   * @param context the transactional context in which to run the query
   * @param isOutermostQuery provide `true` if this is the outer-most query and should close the transaction when finished or error
   * @param profile if `true` run with profiling enabled
   * @param prePopulate if `true` pre populate all results
   * @param subscriber the subscriber where results will be streamed
   * @return a `QueryExecution` that controls the demand to the subscriber
   */
  def executeSubquery(query: String,
                      params: MapValue,
                      context: TransactionalContext,
                      isOutermostQuery: Boolean,
                      profile: Boolean,
                      prePopulate: Boolean,
                      subscriber: QuerySubscriber): QueryExecution = {
    val queryTracer = tracer.compileQuery(query)
    closing(context, queryTracer) {
      val couldContainSensitiveFields = isOutermostQuery && compilerLibrary.supportsAdministrativeCommands()
      val preParsedQuery = preParser.preParseQuery(query, profile, couldContainSensitiveFields)
      doExecute(preParsedQuery, params, context, isOutermostQuery, prePopulate, NoInput, defaultQueryExecutionMonitor, queryTracer, subscriber)
    }
  }

  private def closing[T](context: TransactionalContext, traceEvent: QueryCompilationEvent)(code: => T): T =
    try code catch {
      case t: Throwable =>
        context.rollback()
        throw t
    } finally traceEvent.close()

  private def doExecute(query: InputQuery,
                        params: MapValue,
                        context: TransactionalContext,
                        isOutermostQuery: Boolean,
                        prePopulate: Boolean,
                        input: InputDataStream,
                        queryMonitor: QueryExecutionMonitor,
                        tracer: QueryCompilationEvent,
                        subscriber: QuerySubscriber): QueryExecution = {

    val executableQuery = try {
      getOrCompile(context, query, tracer, params)
    } catch {
      case up: Throwable =>
        if (isOutermostQuery)
          queryMonitor.endFailure(context.executingQuery(), up.getMessage)
        throw up
    }
    if (query.options.queryOptions.executionMode.name != "explain") {
      checkParameters(executableQuery.paramNames, params, executableQuery.extractedParams)
    }
    val combinedParams = params.updatedWith(executableQuery.extractedParams)

    if (isOutermostQuery) {
      context.executingQuery().onObfuscatorReady(executableQuery.queryObfuscator, query.options.offset.offset)
      context.executingQuery().onCompilationCompleted(
        executableQuery.compilerInfo,
        executableQuery.planDescriptionSupplier(),
        executableQuery.deprecationNotificationsProvider(query.options.offset))
    }

    executableQuery.execute(context, isOutermostQuery, query.options, combinedParams, prePopulate, input, queryMonitor, subscriber)
  }

  /*
   * Return the CompilerWithExpressionCodeGenOption to be used.
   */
  private def compilerWithExpressionCodeGenOption(inputQuery: InputQuery,
                                                  tracer: QueryCompilationEvent,
                                                  transactionalContext: TransactionalContext,
                                                  params: MapValue): CompilerWithExpressionCodeGenOption[ExecutableQuery] = {
    val compiledExpressionCompiler = () => masterCompiler.compile(inputQuery.withRecompilationLimitReached,
      tracer, transactionalContext, params)
    val interpretedExpressionCompiler = () => masterCompiler.compile(inputQuery, tracer, transactionalContext, params)

    new CompilerWithExpressionCodeGenOption[ExecutableQuery] {
      override def compile(): ExecutableQuery = {
        if (inputQuery.options.compileWhenHot && config.recompilationLimit == 0) {
          //We have recompilationLimit == 0, go to compiled directly
          compiledExpressionCompiler()
        } else {
          interpretedExpressionCompiler()
        }
      }

      override def compileWithExpressionCodeGen(): ExecutableQuery = compiledExpressionCompiler()

      override def maybeCompileWithExpressionCodeGen(hitCount: Int): Option[ExecutableQuery] = {
        //check if we need to do jit compiling of queries and if hot enough
        if (inputQuery.options.compileWhenHot && config.recompilationLimit > 0 && hitCount >= config.recompilationLimit) {
          Some(compiledExpressionCompiler())
        } else {
          //In the other case we have no recompilation step
          None
        }
      }
    }
  }

  private def getOrCompile(context: TransactionalContext,
                           initialInputQuery: InputQuery,
                           tracer: QueryCompilationEvent,
                           params: MapValue,
                          ): ExecutableQuery = {

    // create transaction and query context
    val tc = context.getOrBeginNewIfClosed()
    val compilerAuthorization = tc.restrictCurrentTransaction(tc.securityContext.withMode(AccessMode.Static.READ))
    var forceReplan = false
    var inputQuery = initialInputQuery

    val cacheKey = CacheKey(
      initialInputQuery.cacheKey,
      QueryCache.extractParameterTypeMap(params),
      tc.kernelTransaction().dataRead().transactionStateHasChanges()
    )

    try {
      var n = 0
      while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {

        val schemaToken = schemaHelper.readSchemaToken(tc)
        if (forceReplan) {
          forceReplan = false
          inputQuery = inputQuery.withReplanOption(CypherReplanOption.force)
        }
        val compiler = compilerWithExpressionCodeGenOption(inputQuery, tracer, tc, params)
        val executableQuery = queryCache.computeIfAbsentOrStale(cacheKey,
          tc,
          compiler,
          inputQuery.options.queryOptions.replan,
          context.executingQuery().id())

        val lockedEntities = schemaHelper.lockEntities(schemaToken, executableQuery, tc)

        if (lockedEntities.successful) {
          return executableQuery
        }
        forceReplan = lockedEntities.needsReplan

        // if the schema has changed while taking all locks we need to try again.
        n += 1
      }
    } finally {
      compilerAuthorization.close()
    }

    throw new IllegalStateException("Could not compile query due to insanely frequent schema changes")
  }

  def clearQueryCaches(): Long =
    List(masterCompiler.clearCaches(), queryCache.clear(), preParser.clearCache()).max

  /**
   * @return { @code true} if the query is a PERIODIC COMMIT query and not an EXPLAIN query
   */
  def isPeriodicCommit(query: String): Boolean = {
    val preParsedQuery = preParser.preParseQuery(query)
    preParsedQuery.options.queryOptions.executionMode != CypherExecutionMode.explain && preParsedQuery.options.isPeriodicCommit
  }

  def getCypherFunctions: java.util.List[FunctionInformation] = {
    val informations: Seq[FunctionInformation] = org.neo4j.cypher.internal.expressions.functions.Function.functionInfo.map(FunctionWithInformation)
    val predicateInformations: Seq[FunctionInformation] = org.neo4j.cypher.internal.expressions.IterablePredicateExpression.functionInfo.map(FunctionWithInformation)
    (informations ++ predicateInformations).asJava
  }

  // HELPERS

  @throws(classOf[ParameterNotFoundException])
  private def checkParameters(queryParams: Array[String], givenParams: MapValue, extractedParams: MapValue) {
    var i = 0
    while (i < queryParams.length) {
      val key = queryParams(i)
      if (!(givenParams.containsKey(key) || extractedParams.containsKey(key))) {
        val missingKeys = queryParams.filter(key => !(givenParams.containsKey(key) || extractedParams.containsKey(key))).distinct
        throw new ParameterNotFoundException("Expected parameter(s): " + missingKeys.mkString(", "))
      }
      i += 1
    }
  }
}

case class FunctionWithInformation(f: FunctionTypeSignature) extends FunctionInformation {

  override def getFunctionName: String = f.function.name

  override def getDescription: String = f.description

  override def getCategory: String = f.category

  override def getSignature: String = f.getSignatureAsString

  override def isAggregationFunction: lang.Boolean = f.isAggregationFunction

  override def returnType: String = f.outputType.toNeoTypeString

  override def inputSignature: java.util.List[java.util.Map[String, String]] = f.names.zip(f.argumentTypes ++ f.optionalTypes).map { case (name, cType) =>
    val typeString = cType.toNeoTypeString
    Map("name" -> name, "type" -> typeString, "description" -> s"$name :: $typeString").asJava
  }.asJava
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
