/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal

import java.lang.Boolean.FALSE
import java.util.{Map => JavaMap}

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{RuntimeJavaValueConverter, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_0.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.v3_0.{LRUCache => LRUCachev3_0, _}
import org.neo4j.cypher.internal.spi.TransactionalContextWrapper
import org.neo4j.cypher.internal.tracing.{CompilationTracer, TimingCompilationTracer}
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api.ReadOperations
import org.neo4j.kernel.api.security.AccessMode
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession, TransactionalContext}
import org.neo4j.kernel.{GraphDatabaseQueryService, api, monitoring}
import org.neo4j.logging.{LogProvider, NullLogProvider}

trait StringCacheMonitor extends CypherCacheMonitor[String, api.Statement]

/**
  * This class construct and initialize both the cypher compiler and the cypher runtime, which is a very expensive
  * operation so please make sure this will be constructed only once and properly reused.
  */
class ExecutionEngine(val queryService: GraphDatabaseQueryService, logProvider: LogProvider = NullLogProvider.getInstance()) {

  require(queryService != null, "Can't work with a null graph database")

  // true means we run inside REST server
  protected val isServer = false
  protected val kernel = queryService.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.api.KernelAPI])
  private val lastCommittedTxId = LastCommittedTxIdProvider(queryService)
  protected val kernelMonitors: monitoring.Monitors = queryService.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
  private val compilationTracer: CompilationTracer = {
    if(optGraphSetting(queryService, GraphDatabaseSettings.cypher_compiler_tracing, FALSE))
      new TimingCompilationTracer(kernelMonitors.newMonitor(classOf[TimingCompilationTracer.EventListener]))
    else
      CompilationTracer.NO_COMPILATION_TRACING
  }
  protected val compiler = createCompiler

  private val log = logProvider.getLog( getClass )
  private val cacheMonitor = kernelMonitors.newMonitor(classOf[StringCacheMonitor])
  kernelMonitors.addMonitorListener( new StringCacheMonitor {
    override def cacheDiscard(ignored: String, query: String) {
      log.info(s"Discarded stale query from the query cache: $query")
    }
  })

  private val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  private val cacheAccessor = new MonitoringCacheAccessor[String, (ExecutionPlan, Map[String, Any])](cacheMonitor)

  private val preParsedQueries = new LRUCachev3_0[String, PreParsedQuery](getPlanCacheSize)
  private val parsedQueries = new LRUCachev3_0[String, ParsedQuery](getPlanCacheSize)

  private val javaValues = new RuntimeJavaValueConverter(isGraphKernelResultValue, identity)
  private val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue, identity)

  @throws(classOf[SyntaxException])
  def profile(query: String, scalaParams: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
    // we got deep scala parameters => convert to deep java parameters
    val javaParams = javaValues.asDeepJavaMap(scalaParams).asInstanceOf[JavaMap[String, AnyRef]]
    profile(query, javaParams, session)
  }

  @throws(classOf[SyntaxException])
  def profile(query: String, javaParams: JavaMap[String, AnyRef], session: QuerySession): ExtendedExecutionResult = {
    // we got deep java parameters => convert to shallow scala parameters for passing into the engine
    val scalaParams = scalaValues.asShallowScalaMap(javaParams)
    executionMonitor.startQueryExecution(session, query, javaParams)
    val (preparedPlanExecution, transactionalContext) = planQuery(query, session)
    preparedPlanExecution.profile(transactionalContext, scalaParams, session)
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, scalaParams: Map[String, Any], session: QuerySession): ExtendedExecutionResult = {
    // we got deep scala parameters => convert to deep java parameters
    val javaParams = javaValues.asDeepJavaMap(scalaParams).asInstanceOf[JavaMap[String, AnyRef]]
    execute(query, javaParams, session)
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, javaParams: JavaMap[String, AnyRef], session: QuerySession): ExtendedExecutionResult = {
    // we got deep java parameters => convert to shallow scala parameters for passing into the engine
    val scalaParams = scalaValues.asShallowScalaMap(javaParams)
    executionMonitor.startQueryExecution(session, query, javaParams)
    val (preparedPlanExecution, transactionalContext) = planQuery(query, session)
    preparedPlanExecution.execute(transactionalContext, scalaParams, session)
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
  protected def planQuery(queryText: String, session: QuerySession): (PreparedPlanExecution, TransactionalContextWrapper) = {
    val phaseTracer = compilationTracer.compileQuery(queryText)
    try {

      val externalTransactionalContext = TransactionalContextWrapper(session.get(TransactionalContext.METADATA_KEY))
      val preParsedQuery = try {
        preParseQuery(queryText)
      } catch {
        case e: SyntaxException =>
          externalTransactionalContext.close(success = false)
          throw e
      }
      val executionMode = preParsedQuery.executionMode
      val cacheKey = preParsedQuery.statementWithVersionAndPlanner

      var n = 0
      while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
        // create transaction and query context
        val tc = externalTransactionalContext.provideContext()

        // Temporarily change access mode during query planning
        val revertable = tc.restrictCurrentTransaction(AccessMode.Static.READ)

        val ((plan: ExecutionPlan, extractedParameters), touched) = try {
          // fetch plan cache
          val cache = getOrCreateFromSchemaState(tc.readOperations, {
            cacheMonitor.cacheFlushDetected(tc.statement)
            val lruCache = new LRUCachev3_0[String, (ExecutionPlan, Map[String, Any])](getPlanCacheSize)
            new QueryCache(cacheAccessor, lruCache)
          })

          def isStale(plan: ExecutionPlan, ignored: Map[String, Any]) = plan.isStale(lastCommittedTxId, tc)
          def producePlan() = {
            val parsedQuery = parsePreParsedQuery(preParsedQuery, phaseTracer)
            parsedQuery.plan(tc, phaseTracer)
          }

          cache.getOrElseUpdate(cacheKey, queryText, (isStale _).tupled, producePlan())
        }
        catch {
          case (t: Throwable) =>
            tc.close(success = false)
            throw t
        } finally {
          revertable.close()
        }

        if (touched) {
          tc.close(success = true)
        } else {
          tc.cleanForReuse()
          return (PreparedPlanExecution(plan, executionMode, extractedParameters), tc)
        }

        n += 1
      }
    } finally phaseTracer.close()

    throw new IllegalStateException("Could not execute query due to insanely frequent schema changes")
  }

  private def getOrCreateFromSchemaState[V](operations: ReadOperations, creator: => V) = {
    val javaCreator = new java.util.function.Function[ExecutionEngine, V]() {
      def apply(key: ExecutionEngine) = creator
    }
    operations.schemaStateGetOrCreate(this, javaCreator)
  }

  def prettify(query: String): String = Prettifier(query)

  def isPeriodicCommit(query: String) = parseQuery(query).isPeriodicCommit

  private def createCompiler: CypherCompiler = {
    val version = CypherVersion(optGraphSetting[String](
      queryService, GraphDatabaseSettings.cypher_parser_version, CypherVersion.default.name))
    val planner = CypherPlanner(optGraphSetting[String](
      queryService, GraphDatabaseSettings.cypher_planner, CypherPlanner.default.name))
    val runtime = CypherRuntime(optGraphSetting[String](
      queryService, GraphDatabaseSettings.cypher_runtime, CypherRuntime.default.name))
    val useErrorsOverWarnings = optGraphSetting[java.lang.Boolean](
      queryService, GraphDatabaseSettings.cypher_hints_error,
      GraphDatabaseSettings.cypher_hints_error.getDefaultValue.toBoolean)
    val idpMaxTableSize: Int = optGraphSetting[java.lang.Integer](
      queryService, GraphDatabaseSettings.cypher_idp_solver_table_threshold,
      GraphDatabaseSettings.cypher_idp_solver_table_threshold.getDefaultValue.toInt)
    val idpIterationDuration: Long = optGraphSetting[java.lang.Long](
      queryService, GraphDatabaseSettings.cypher_idp_solver_duration_threshold,
      GraphDatabaseSettings.cypher_idp_solver_duration_threshold.getDefaultValue.toLong)

    val errorIfShortestPathFallbackUsedAtRuntime = optGraphSetting[java.lang.Boolean](
      queryService, GraphDatabaseSettings.forbid_exhaustive_shortestpath,
      GraphDatabaseSettings.forbid_exhaustive_shortestpath.getDefaultValue.toBoolean
    )
    val errorIfShortestPathHasCommonNodesAtRuntime = optGraphSetting[java.lang.Boolean](
      queryService, GraphDatabaseSettings.forbid_shortestpath_common_nodes,
      GraphDatabaseSettings.forbid_shortestpath_common_nodes.getDefaultValue.toBoolean
    )

    if (((version != CypherVersion.v2_3) || (version != CypherVersion.v3_0)) && (planner == CypherPlanner.greedy || planner == CypherPlanner.idp || planner == CypherPlanner.dp)) {
      val message = s"Cannot combine configurations: ${GraphDatabaseSettings.cypher_parser_version.name}=${version.name} " +
        s"with ${GraphDatabaseSettings.cypher_planner.name} = ${planner.name}"
      log.error(message)
      throw new IllegalStateException(message)
    }
    new CypherCompiler(queryService, kernel, kernelMonitors, version, planner, runtime, useErrorsOverWarnings, idpMaxTableSize, idpIterationDuration, errorIfShortestPathFallbackUsedAtRuntime, errorIfShortestPathHasCommonNodesAtRuntime, logProvider)
  }

  private def getPlanCacheSize: Int =
    optGraphSetting[java.lang.Integer](
      queryService, GraphDatabaseSettings.query_cache_size,
      GraphDatabaseSettings.query_cache_size.getDefaultValue.toInt
    )

  private def optGraphSetting[V](graph: GraphDatabaseQueryService, setting: Setting[V], defaultValue: V): V = {
    val config = graph.getDependencyResolver.resolveDependency(classOf[Config])
    Option(config.get(setting)).getOrElse(defaultValue)
  }

}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
