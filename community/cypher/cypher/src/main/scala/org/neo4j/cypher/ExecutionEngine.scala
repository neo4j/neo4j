/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserMonitor
import org.neo4j.cypher.internal.compiler.v2_2.prettifier.Prettifier
import org.neo4j.cypher.internal.compiler.v2_2.{CypherCacheMonitor, MonitoringCacheAccessor}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore
import org.neo4j.kernel.impl.util.StringLogger
import org.neo4j.kernel.{GraphDatabaseAPI, InternalAbstractGraphDatabase, api}

import scala.collection.JavaConverters._

trait StringCacheMonitor extends CypherCacheMonitor[String, api.Statement]

class ExecutionEngine(graph: GraphDatabaseService, logger: StringLogger = StringLogger.DEV_NULL) {

  require(graph != null, "Can't work with a null graph database")

  // true means we run inside REST server
  protected val isServer = false
  protected val graphAPI = graph.asInstanceOf[GraphDatabaseAPI]
  protected val kernel = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.api.KernelAPI])
  private val lastTxId: () => Long =
      graphAPI.getDependencyResolver.resolveDependency( classOf[TransactionIdStore]).getLastCommittedTransactionId
  protected val kernelMonitors = graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
  protected val compiler = createCompiler(logger)

  private val cacheMonitor = kernelMonitors.newMonitor(classOf[StringCacheMonitor])
  kernelMonitors.addMonitorListener( new StringCacheMonitor {
    override def cacheDiscard(query: String) {
      logger.info(s"Discarded stale query from the query cache: $query")
    }
  })
  private val cacheAccessor = new MonitoringCacheAccessor[String, (ExecutionPlan, Map[String, Any])](cacheMonitor)

  private val parsedQueries = new LRUCache[String, ParsedQuery](getPlanCacheSize)

  @throws(classOf[SyntaxException])
  def profile(query: String): ExtendedExecutionResult = profile(query, Map[String, Any]())

  @throws(classOf[SyntaxException])
  def profile(query: String, params: JavaMap[String, Any]): ExtendedExecutionResult = profile(query, params.asScala.toMap)

  @throws(classOf[SyntaxException])
  def profile(query: String, params: Map[String, Any]): ExtendedExecutionResult = {
    val (plan, extractedParams, txInfo) = planQuery(query)
    plan.profile(graphAPI, txInfo, params ++ extractedParams)
  }

  @throws(classOf[SyntaxException])
  def execute(query: String): ExtendedExecutionResult = execute(query, Map[String, Any]())

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExtendedExecutionResult = execute(query, params.asScala.toMap)

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any]): ExtendedExecutionResult = {
    val (plan, extractedParams, txInfo) = planQuery(query)
    plan.execute(graphAPI, txInfo, params ++ extractedParams)
  }

  @throws(classOf[SyntaxException])
  protected def parseQuery(queryText: String): ParsedQuery =
    parsedQueries.getOrElseUpdate( queryText, compiler.parseQuery( queryText ) )

  @throws(classOf[SyntaxException])
  protected def planQuery(queryText: String): (ExecutionPlan, Map[String, Any], TransactionInfo) = {
    logger.debug(queryText)
    var n = 0
    while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
      // create transaction and query context
      var touched = false
      val isTopLevelTx = !txBridge.hasTransaction
      val tx = graph.beginTx()
      val kernelStatement = txBridge.instance()

      val (plan: ExecutionPlan, extractedParameters) = try {
        // fetch plan cache
        val cache: LRUCache[String, (ExecutionPlan, Map[String, Any])] = getOrCreateFromSchemaState(kernelStatement, {
          cacheMonitor.cacheFlushDetected(kernelStatement)
          new LRUCache[String, (ExecutionPlan, Map[String, Any])](getPlanCacheSize)
        })

        Iterator.continually {
          cacheAccessor.getOrElseUpdate(cache)(queryText, {
            touched = true
            val parsedQuery: ParsedQuery = parseQuery(queryText)
            parsedQuery.plan(kernelStatement)
          })
        }.flatMap { case (plan, params) =>
          if ( !touched && plan.isStale(lastTxId, kernelStatement)) {
            cacheAccessor.remove(cache)(queryText)
            None
          } else {
            Some((plan, params))
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
        return (plan, extractedParameters, TransactionInfo(tx, isTopLevelTx, txBridge.instance()))
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

  private def createCompiler(logger: StringLogger): CypherCompiler = {
    val version = optGraphSetting[String](
      graph, GraphDatabaseSettings.cypher_parser_version, CypherVersion.vDefault.name
    )
    val optionParser = CypherOptionParser(kernelMonitors.newMonitor(classOf[ParserMonitor[CypherQueryWithOptions]]))
    new CypherCompiler(graph, kernel, kernelMonitors, CypherVersion(version), optionParser, logger)
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

    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(g => Option(g.getConfig.get(setting)))
      .andThen(_.getOrElse(defaultValue))
      .applyOrElse(graph, (_: GraphDatabaseService) => defaultValue)
  }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
