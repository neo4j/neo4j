/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher

import java.lang.Boolean.FALSE
import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.v2_3.{LRUCache => LRUCachev2_3, _}
import org.neo4j.cypher.internal.frontend.v2_3.helpers.JavaCompatibility.asJavaMap
import org.neo4j.cypher.internal.tracing.{CompilationTracer, TimingCompilationTracer}
import org.neo4j.cypher.internal.{CypherCompiler, _}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.{QueryEngineProvider, QueryExecutionMonitor, QuerySession}
import org.neo4j.kernel.{GraphDatabaseAPI, api, monitoring}
import org.neo4j.logging.{LogProvider, NullLogProvider}

import scala.collection.JavaConverters._

trait StringCacheMonitor extends CypherCacheMonitor[String, api.Statement]
/**
  * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
  * operation so please make sure this will be constructed only once and properly reused.
  *
  * @deprecated use { @link org.neo4j.graphdb.GraphDatabaseService#execute(String)} instead.
  */
@Deprecated
class ExecutionEngine(graph: GraphDatabaseService, logProvider: LogProvider = NullLogProvider.getInstance()) {

  require(graph != null, "Can't work with a null graph database")

  // true means we run inside REST server
  protected val isServer = false
  protected val graphAPI = graph.asInstanceOf[GraphDatabaseAPI]
  protected val kernel = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.api.KernelAPI])
  private val lastCommittedTxId = LastCommittedTxIdProvider(graphAPI)
  protected val kernelMonitors: monitoring.Monitors = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
  private val compilationTracer: CompilationTracer = {
    if(optGraphSetting(graph, GraphDatabaseSettings.cypher_compiler_tracing, FALSE))
      new TimingCompilationTracer(kernelMonitors.newMonitor(classOf[TimingCompilationTracer.EventListener]))
    else
      CompilationTracer.NO_COMPILATION_TRACING
  }
  protected val compiler = createCompiler

  private val log = logProvider.getLog( getClass )
  private val cacheMonitor = kernelMonitors.newMonitor(classOf[StringCacheMonitor])
  kernelMonitors.addMonitorListener(new StringCacheMonitor {
    override def cacheDiscard(query: String) {
      log.info(s"Discarded stale query from the query cache: $query")
    }
  })

  private val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  private val cacheAccessor = new MonitoringCacheAccessor[String, (ExecutionPlan, Map[String, Any])](cacheMonitor)

  private val preParsedQueries = new LRUCachev2_3[String, PreParsedQuery](getPlanCacheSize)
  private val parsedQueries = new LRUCachev2_3[String, ParsedQuery](getPlanCacheSize)

  @throws(classOf[SyntaxException])
  def profile(query: String): ExtendedExecutionResult = profile(query, Map[String, Any](), QueryEngineProvider.embeddedSession)

  @throws(classOf[SyntaxException])
  def profile(query: String, params: JavaMap[String, Any]): ExtendedExecutionResult = profile(query, params.asScala.toMap, QueryEngineProvider.embeddedSession)

  @throws(classOf[SyntaxException])
  def profile(query: String, params: Map[String, Any]): ExtendedExecutionResult = profile(query, params, QueryEngineProvider.embeddedSession)

  @throws(classOf[SyntaxException])
  def profile(query: String, params: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
    executionMonitor.startQueryExecution(session, query, asJavaMap(params))

    val (preparedPlanExecution, txInfo) = planQuery(query)
    preparedPlanExecution.profile(graphAPI, txInfo, params, session)
  }

  @throws(classOf[SyntaxException])
  def profile(query: String, params: JavaMap[String, Any], session: QuerySession): ExtendedExecutionResult =
    profile(query, params.asScala.toMap, session)

  @throws(classOf[SyntaxException])
  def execute(query: String): ExtendedExecutionResult = execute(query, Map[String, Any]())

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExtendedExecutionResult = execute(query, params.asScala.toMap, QueryEngineProvider.embeddedSession)

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any]): ExtendedExecutionResult =
    execute(query, params, QueryEngineProvider.embeddedSession)

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any], session: QuerySession): ExtendedExecutionResult =
    execute(query, params.asScala.toMap, session)

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
    executionMonitor.startQueryExecution(session, query, asJavaMap(params))
    val (preparedPlanExecution, txInfo) = planQuery(query)
    preparedPlanExecution.execute(graphAPI, txInfo, params, session)
  }

  @throws(classOf[SyntaxException])
  protected def parseQuery(queryText: String): ParsedQuery =
    parsePreParsedQuery(preParseQuery(queryText), CompilationPhaseTracer.NO_TRACING)

  @throws(classOf[SyntaxException])
  private def parsePreParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer): ParsedQuery = {
    parsedQueries.get(preParsedQuery.statementWithVersionAndPlanner).getOrElse {
      val parsedQuery = compiler.parseQuery(preParsedQuery, tracer)
      //don't cache failed queries
      if (!parsedQuery.hasErrors) parsedQueries.put(preParsedQuery.statementWithVersionAndPlanner, parsedQuery)
      parsedQuery
    }
  }

  @throws(classOf[SyntaxException])
  private def preParseQuery(queryText: String): PreParsedQuery =
    preParsedQueries.getOrElseUpdate(queryText, compiler.preParseQuery(queryText))

  @throws(classOf[SyntaxException])
  protected def planQuery(queryText: String): (PreparedPlanExecution, TransactionInfo) = {
    val phaseTracer = compilationTracer.compileQuery(queryText)
    try {

      val preParsedQuery = preParseQuery(queryText)
      val executionMode = preParsedQuery.executionMode
      val cacheKey = preParsedQuery.statementWithVersionAndPlanner

      var n = 0
      while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
        // create transaction and query context
        val isTopLevelTx = !txBridge.hasTransaction
        val tx = graph.beginTx()
        val kernelStatement = txBridge.get()

        val ((plan: ExecutionPlan, extractedParameters), touched) = try {
          // fetch plan cache
          val cache = getOrCreateFromSchemaState(kernelStatement, {
            cacheMonitor.cacheFlushDetected(kernelStatement)
            val lruCache = new LRUCachev2_3[String, (ExecutionPlan, Map[String, Any])](getPlanCacheSize)
            new QueryCache[String, (ExecutionPlan, Map[String, Any])](cacheAccessor, lruCache)
          })

          cache.getOrElseUpdate(cacheKey,
            planParams => {
              val stale: Boolean = planParams._1.isStale(lastCommittedTxId, kernelStatement)
              stale
            }, {
              val parsedQuery = parsePreParsedQuery(preParsedQuery, phaseTracer)
              parsedQuery.plan(kernelStatement, phaseTracer)
            }
          )
        }
        catch {
          case (t: Throwable) =>
            kernelStatement.close()
            tx.failure()
            tx.close()
            throw t
        }

        if (touched) {
          kernelStatement.close()
          tx.success()
          tx.close()
        } else {
          // close the old statement reference after the statement has been "upgraded"
          // to either a schema data or a schema statement, so that the locks are "handed over".
          kernelStatement.close()
          val preparedPlanExecution = PreparedPlanExecution(plan, executionMode, extractedParameters)
          val txInfo = TransactionInfo(tx, isTopLevelTx, txBridge.get())
          return (preparedPlanExecution, txInfo)
        }

        n += 1
      }
    } finally phaseTracer.close()

    throw new IllegalStateException("Could not execute query due to insanely frequent schema changes")
  }

  private val txBridge = graph.asInstanceOf[GraphDatabaseAPI]
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])

  private def getOrCreateFromSchemaState[V](statement: api.Statement, creator: => V) = {
    val javaCreator = new org.neo4j.function.Function[ExecutionEngine, V]() {
      def apply(key: ExecutionEngine) = creator
    }
    statement.readOperations().schemaStateGetOrCreate(this, javaCreator)
  }

  def prettify(query: String): String = Prettifier(query)

  private def createCompiler: CypherCompiler = {
    val version = CypherVersion(optGraphSetting[String](
      graph, GraphDatabaseSettings.cypher_parser_version, CypherVersion.default.name))
    val planner = CypherPlanner(optGraphSetting[String](
      graph, GraphDatabaseSettings.cypher_planner, CypherPlanner.default.name))
    val runtime = CypherRuntime(optGraphSetting[String](
      graph, GraphDatabaseSettings.cypher_runtime, CypherRuntime.default.name))
    val useErrorsOverWarnings: java.lang.Boolean = optGraphSetting[java.lang.Boolean](
      graph, GraphDatabaseSettings.cypher_hints_error,
      GraphDatabaseSettings.cypher_hints_error.getDefaultValue.toBoolean)
    val idpMaxTableSize: Int = optGraphSetting[java.lang.Integer](
      graph, GraphDatabaseSettings.cypher_idp_solver_table_threshold,
      GraphDatabaseSettings.cypher_idp_solver_table_threshold.getDefaultValue.toInt)
    val idpIterationDuration: Long = optGraphSetting[java.lang.Long](
      graph, GraphDatabaseSettings.cypher_idp_solver_duration_threshold,
      GraphDatabaseSettings.cypher_idp_solver_duration_threshold.getDefaultValue.toLong)
    if ((version != CypherVersion.v2_2 && version != CypherVersion.v2_3) && (planner == CypherPlanner.greedy || planner == CypherPlanner.idp || planner == CypherPlanner.dp)) {
      val message = s"Cannot combine configurations: ${GraphDatabaseSettings.cypher_parser_version.name}=${version.name} " +
        s"with ${GraphDatabaseSettings.cypher_planner.name} = ${planner.name}"
      log.error(message)
      throw new IllegalStateException(message)
    }
    new CypherCompiler(graph, kernel, kernelMonitors, version, planner, runtime, useErrorsOverWarnings, idpMaxTableSize, idpIterationDuration, logProvider)
  }

  private def getPlanCacheSize: Int =
    optGraphSetting[java.lang.Integer](
      graph, GraphDatabaseSettings.query_cache_size,
      GraphDatabaseSettings.query_cache_size.getDefaultValue.toInt
    )

  private def optGraphSetting[V](graph: GraphDatabaseService, setting: Setting[V], defaultValue: V): V = {
    def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
      case (db: T) => db
    }
    optGraphAs[GraphDatabaseFacade]
      .andThen(g => {
      Option(g.platformModule.config.get(setting))
    })
      .andThen(_.getOrElse(defaultValue))
      .applyOrElse(graph, (_: GraphDatabaseService) => defaultValue)
  }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
