/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.{Map => JavaMap}

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.parser.ParserMonitor
import org.neo4j.cypher.internal.compiler.v2_3.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.v2_3.{LRUCache => LRUCachev2_3, _}
import org.neo4j.cypher.internal.{CypherCompiler, _}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.logging.NullLogProvider
import org.neo4j.kernel.impl.query.{QueryEngineProvider, QueryExecutionMonitor, QuerySession}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore
import org.neo4j.kernel.{GraphDatabaseAPI, api, monitoring}
import org.neo4j.logging.LogProvider

import scala.collection.JavaConverters._

trait StringCacheMonitor extends CypherCacheMonitor[String, api.Statement]

class ExecutionEngine(graph: GraphDatabaseService, logProvider: LogProvider = NullLogProvider.getInstance()) {

  require(graph != null, "Can't work with a null graph database")

  // true means we run inside REST server
  protected val isServer = false
  protected val graphAPI = graph.asInstanceOf[GraphDatabaseAPI]
  protected val kernel = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.api.KernelAPI])
  private val lastTxId: () => Long =
      graphAPI.getDependencyResolver.resolveDependency( classOf[TransactionIdStore]).getLastCommittedTransactionId
  protected val kernelMonitors: monitoring.Monitors = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
  protected val compiler = createCompiler

  private val log = logProvider.getLog( getClass )
  private val cacheMonitor = kernelMonitors.newMonitor(classOf[StringCacheMonitor])
  kernelMonitors.addMonitorListener( new StringCacheMonitor {
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
  def profile(query: String, params: Map[String, Any],session: QuerySession): ExtendedExecutionResult = {
    executionMonitor.startQueryExecution(session, query)

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
    executionMonitor.startQueryExecution(session, query)
    val (preparedPlanExecution, txInfo) = planQuery(query)
    preparedPlanExecution.execute(graphAPI, txInfo, params, session)
  }

  @throws(classOf[SyntaxException])
  protected def parseQuery(queryText: String): ParsedQuery =
    parsePreParsedQuery(preParseQuery(queryText))

  @throws(classOf[SyntaxException])
  private def parsePreParsedQuery(preParsedQuery: PreParsedQuery): ParsedQuery =
    parsedQueries.getOrElseUpdate(preParsedQuery.statementWithVersionAndPlanner, compiler.parseQuery(preParsedQuery))

  @throws(classOf[SyntaxException])
  private def preParseQuery(queryText: String): PreParsedQuery =
    preParsedQueries.getOrElseUpdate(queryText, compiler.preParseQuery(queryText))

  @throws(classOf[SyntaxException])
  protected def planQuery(queryText: String): (PreparedPlanExecution, TransactionInfo) = {
    log.debug(queryText)

    val preParsedQuery = preParseQuery(queryText)
    val executionMode = preParsedQuery.executionMode
    val cacheKey = preParsedQuery.statementWithVersionAndPlanner

    var n = 0
    while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
      // create transaction and query context
      var touched = false
      val isTopLevelTx = !txBridge.hasTransaction
      val tx = graph.beginTx()
      val kernelStatement = txBridge.get()

      val (plan: ExecutionPlan, extractedParameters) = try {
        // fetch plan cache
        val cache: LRUCachev2_3[String, (ExecutionPlan, Map[String, Any])] = getOrCreateFromSchemaState(kernelStatement, {
          cacheMonitor.cacheFlushDetected(kernelStatement)
          new LRUCachev2_3[String, (ExecutionPlan, Map[String, Any])](getPlanCacheSize)
        })

        Iterator.continually {
          cacheAccessor.getOrElseUpdate(cache)(cacheKey, {
            touched = true
            val parsedQuery = parsePreParsedQuery(preParsedQuery)
            parsedQuery.plan(kernelStatement)
          })
        }.flatMap { case (candidatePlan, params) =>
          if (!touched && candidatePlan.isStale(lastTxId, kernelStatement)) {
            cacheAccessor.remove(cache)(cacheKey)
            None
          } else {
            Some((candidatePlan, params))
          }
        }.next()
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
      }
      else {
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        kernelStatement.close()
        val preparedPlanExecution = PreparedPlanExecution(plan, executionMode, extractedParameters)
        val txInfo = TransactionInfo(tx, isTopLevelTx, txBridge.get())
        return (preparedPlanExecution, txInfo)
      }

      n += 1
    }

    throw new IllegalStateException("Could not execute query due to insanely frequent schema changes")
  }

  private val txBridge = graph.asInstanceOf[GraphDatabaseAPI]
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])

  private def getOrCreateFromSchemaState[V](statement: api.Statement, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[ExecutionEngine, V]() {
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
    if ((version != CypherVersion.v2_2 && version != CypherVersion.v2_3) && (planner == CypherPlanner.greedy || planner == CypherPlanner.idp || planner == CypherPlanner.dp)) {
      val message = s"Cannot combine configurations: ${GraphDatabaseSettings.cypher_parser_version.name}=${version.name} " +
        s"with ${GraphDatabaseSettings.cypher_planner.name} = ${planner.name}"
      log.error(message)
      throw new IllegalStateException(message)
    }
    val optionParser = CypherPreParser(kernelMonitors.newMonitor(classOf[ParserMonitor[PreParsedStatement]]))
    new CypherCompiler(graph, kernel, kernelMonitors, version, planner, runtime, optionParser, logProvider)
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
