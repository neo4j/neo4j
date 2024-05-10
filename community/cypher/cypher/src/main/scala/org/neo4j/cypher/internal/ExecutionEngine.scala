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

import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.CompilationTracer.QueryCompilationEvent
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.exceptions.ParameterNotFoundException
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.impl.query.FunctionInformation
import org.neo4j.kernel.impl.query.FunctionInformation.InputInformation
import org.neo4j.kernel.impl.query.QueryExecution
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode
import org.neo4j.logging.InternalLogProvider
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.lang
import java.time.Clock
import java.util.Optional

import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * This class constructs and initializes both the cypher compilers and runtimes, which are very expensive
 * operation. Please make sure this will be constructed only once and properly reused.
 */
abstract class ExecutionEngine(
  val queryService: GraphDatabaseQueryService,
  val kernelMonitors: Monitors,
  val tracer: CompilationTracer,
  val config: CypherConfiguration,
  val masterCompiler: MasterCompiler,
  val queryCaches: CypherQueryCaches,
  val logProvider: InternalLogProvider,
  val clock: Clock = Clock.systemUTC()
) {

  // HELPER OBJECTS
  protected val defaultQueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  private val preParser = new CachingPreParser(config, queryCaches.preParserCache)

  private val queryCache: QueryCache[CacheKey[InputQuery.CacheKey], ExecutableQuery] = queryCaches.executableQueryCache

  private val schemaHelper = new SchemaHelper(queryCache, masterCompiler)

  // ACTUAL FUNCTIONALITY

  /**
   * Executes query returns a `QueryExecution` that can be used to control demand to the provided `QuerySubscriber`.
   * This method assumes this is the only query running within the transaction, and therefor will register transaction closing
   * with the TaskCloser
   *
   * @param query       the query to execute
   * @param params      the parameters of the query
   * @param context     the transactional context in which to run the query
   * @param profile     if `true` run with profiling enabled
   * @param prePopulate if `true` pre populate all results
   * @param subscriber  the subscriber where results will be streamed
   * @return a `QueryExecution` that controls the demand to the subscriber
   */
  def execute(
    query: String,
    params: MapValue,
    context: TransactionalContext,
    profile: Boolean,
    prePopulate: Boolean,
    subscriber: QuerySubscriber,
    monitor: QueryExecutionMonitor = defaultQueryExecutionMonitor
  ): QueryExecution = {
    monitor.startProcessing(context.executingQuery())
    executeSubquery(query, params, context, isOutermostQuery = true, profile, prePopulate, subscriber, monitor)
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
  def execute(
    query: FullyParsedQuery,
    params: MapValue,
    context: TransactionalContext,
    prePopulate: Boolean,
    input: InputDataStream,
    queryMonitor: QueryExecutionMonitor,
    subscriber: QuerySubscriber
  ): QueryExecution = {
    queryMonitor.startProcessing(context.executingQuery())
    val queryTracer = tracer.compileQuery(query.description)
    val notificationLogger = new RecordingNotificationLogger()
    closing(context, queryTracer) {
      doExecute(
        query,
        params,
        context,
        isOutermostQuery = true,
        prePopulate,
        input,
        queryMonitor,
        queryTracer,
        subscriber,
        notificationLogger
      )
    }
  }

  /**
   * Executes query returns a `QueryExecution` that can be used to control demand to the provided `QuerySubscriber`.
   * This method assumes the query is running as one of many queries within a single transaction and therefor needs
   * to be told using the shouldCloseTransaction field if the TaskCloser needs to have a transaction close registered.
   *
   * @param query            the query to execute
   * @param params           the parameters of the query
   * @param context          the transactional context in which to run the query
   * @param isOutermostQuery provide `true` if this is the outer-most query and should close the transaction when finished or error
   * @param profile          if `true` run with profiling enabled
   * @param prePopulate      if `true` pre populate all results
   * @param subscriber       the subscriber where results will be streamed
   * @return a `QueryExecution` that controls the demand to the subscriber
   */
  def executeSubquery(
    query: String,
    params: MapValue,
    context: TransactionalContext,
    isOutermostQuery: Boolean,
    profile: Boolean,
    prePopulate: Boolean,
    subscriber: QuerySubscriber,
    monitor: QueryExecutionMonitor = defaultQueryExecutionMonitor
  ): QueryExecution = {
    val queryTracer = tracer.compileQuery(query)
    closing(context, queryTracer) {
      val couldContainSensitiveFields = isOutermostQuery && masterCompiler.supportsAdministrativeCommands()
      val notificationLogger = new RecordingNotificationLogger()
      val preParsedQuery = preParser.preParseQuery(
        query,
        notificationLogger,
        profile,
        couldContainSensitiveFields,
        DatabaseMode.COMPOSITE.equals(context.databaseMode())
      )
      doExecute(
        preParsedQuery,
        params,
        context,
        isOutermostQuery,
        prePopulate,
        NoInput,
        monitor,
        queryTracer,
        subscriber,
        notificationLogger
      )
    }
  }

  private def closing[T](context: TransactionalContext, traceEvent: QueryCompilationEvent)(code: => T): T =
    try code
    catch {
      case e: HasStatus =>
        context.kernelTransaction().markForTermination(e.status())
        context.close()
        throw e;

      case t: Throwable =>
        context.kernelTransaction().markForTermination(Status.Transaction.QueryExecutionFailedOnTransaction)
        context.close()
        throw t

    } finally traceEvent.close()

  private def doExecute(
    query: InputQuery,
    params: MapValue,
    context: TransactionalContext,
    isOutermostQuery: Boolean,
    prePopulate: Boolean,
    input: InputDataStream,
    queryMonitor: QueryExecutionMonitor,
    tracer: QueryCompilationEvent,
    subscriber: QuerySubscriber,
    notificationLogger: InternalNotificationLogger
  ): QueryExecution = {

    val executableQuery =
      try {
        getOrCompile(context, query, tracer, params, notificationLogger)
      } catch {
        case up: Throwable =>
          if (isOutermostQuery) {
            val status = up match {
              case withStatus: HasStatus => withStatus.status()
              case _                     => null
            }
            queryMonitor.endFailure(context.executingQuery(), up.getMessage, status)
          }
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
        executableQuery.planDescriptionSupplier()
      )
    }

    executableQuery.execute(
      context,
      isOutermostQuery,
      query.options,
      combinedParams,
      prePopulate,
      input,
      queryMonitor,
      subscriber
    )
  }

  /*
   * Return the CompilerWithExpressionCodeGenOption to be used.
   */
  private def compilerWithExpressionCodeGenOption(
    inputQuery: InputQuery,
    tracer: QueryCompilationEvent,
    transactionalContext: TransactionalContext,
    params: MapValue,
    notificationLogger: InternalNotificationLogger
  ): CompilerWithExpressionCodeGenOption[ExecutableQuery] = {
    val compiledExpressionCompiler =
      () =>
        masterCompiler.compile(
          inputQuery.withRecompilationLimitReached,
          tracer,
          transactionalContext,
          params,
          notificationLogger
        )
    val interpretedExpressionCompiler =
      () =>
        masterCompiler.compile(
          inputQuery,
          tracer,
          transactionalContext,
          params,
          notificationLogger
        )

    new CompilerWithExpressionCodeGenOption[ExecutableQuery] {
      override def compile(): ExecutableQuery = {
        if (inputQuery.options.compileWhenHot && config.recompilationLimit == 0) {
          // We have recompilationLimit == 0, go to compiled directly
          compiledExpressionCompiler()
        } else {
          interpretedExpressionCompiler()
        }
      }

      override def compileWithExpressionCodeGen(): ExecutableQuery = compiledExpressionCompiler()

      override def maybeCompileWithExpressionCodeGen(hitCount: Int): Option[ExecutableQuery] = {
        // check if we need to do jit compiling of queries and if hot enough
        if (
          inputQuery.options.compileWhenHot && config.recompilationLimit > 0 && hitCount >= config.recompilationLimit
        ) {
          Some(compiledExpressionCompiler())
        } else {
          // In the other case we have no recompilation step
          None
        }
      }
    }
  }

  private def getOrCompile(
    context: TransactionalContext,
    initialInputQuery: InputQuery,
    tracer: QueryCompilationEvent,
    params: MapValue,
    notificationLogger: InternalNotificationLogger
  ): ExecutableQuery = {

    // create transaction and query context
    val tc = context.getOrBeginNewIfClosed()
    val compilerAuthorization = tc.restrictCurrentTransaction(tc.securityContext.withMode(AccessMode.Static.READ))
    var forceReplan = false
    var inputQuery = initialInputQuery

    val cacheKey = CacheKey(
      initialInputQuery.cacheKey,
      QueryCache.extractParameterTypeMap(params, config.useParameterSizeHint),
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
        val compiler = compilerWithExpressionCodeGenOption(inputQuery, tracer, tc, params, notificationLogger)
        val executableQuery = queryCache.computeIfAbsentOrStale(
          cacheKey,
          tc,
          compiler,
          inputQuery.options.queryOptions.replan,
          context.executingQuery().id()
        )

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

  def clearPreParserCache(): Long =
    preParser.clearCache()

  def clearExecutableQueryCache(): Long =
    queryCache.clear()

  def clearCompilerCaches(): Long =
    masterCompiler.clearCaches()

  def insertIntoCache(
    queryText: String,
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = {
    preParser.insertIntoCache(queryText, preParsedQuery)
    masterCompiler.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
  }

  def getCypherFunctions: java.util.List[FunctionInformation] = {
    val informations: Seq[FunctionInformation] =
      org.neo4j.cypher.internal.expressions.functions.Function.functionInfo.map(FunctionWithInformation)
    val predicateInformations: Seq[FunctionInformation] =
      org.neo4j.cypher.internal.expressions.IterablePredicateExpression.functionInfo.map(FunctionWithInformation)
    (informations ++ predicateInformations).asJava
  }

  // HELPERS

  @throws(classOf[ParameterNotFoundException])
  private def checkParameters(queryParams: Array[String], givenParams: MapValue, extractedParams: MapValue): Unit = {
    var i = 0
    while (i < queryParams.length) {
      val key = queryParams(i)
      if (!(givenParams.containsKey(key) || extractedParams.containsKey(key))) {
        val missingKeys =
          queryParams.filter(key => !(givenParams.containsKey(key) || extractedParams.containsKey(key))).distinct
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

  override def isDeprecated: lang.Boolean = f.deprecated

  override def deprecatedBy: java.util.Optional[lang.String] = Optional.ofNullable(f.deprecatedBy.orNull)

  override def returnType: String = f.outputType.normalizedCypherTypeString()

  override def inputSignature: java.util.List[InputInformation] = {
    f.names.zip(f.argumentTypes ++ f.optionalTypes).map { case (name, cType) =>
      val typeString = f.overriddenArgumentTypeName match {
        case Some(map) => map.getOrElse(name, cType.normalizedCypherTypeString())
        case None      => cType.normalizedCypherTypeString()
      }
      new InputInformation(
        name,
        typeString,
        s"$name :: $typeString",
        false,
        java.util.Optional.empty[String]()
      )
    }.asJava
  }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
