/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.internal.{ExecutionPlan, CypherCompiler, LRUCache}
import internal.prettifier.Prettifier
import internal.spi.gdsimpl.{TransactionBoundExecutionContext, TransactionBoundPlanContext}
import internal.spi.{ExceptionTranslatingQueryContext, QueryContext}
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI, InternalAbstractGraphDatabase}
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.util.StringLogger
import org.neo4j.kernel.api.Statement
import scala.collection.JavaConverters._
import java.util.{Map => JavaMap}

class ExecutionEngine(graph: GraphDatabaseService, logger: StringLogger = StringLogger.DEV_NULL) {

  require(graph != null, "Can't work with a null graph database")

  val compiler = createCorrectCompiler()

  @throws(classOf[SyntaxException])
  def profile(query: String, params: Map[String, Any]): ExecutionResult = {
    logger.debug(query)
    prepare(query, { (plan: ExecutionPlan[QueryContext], queryContext: QueryContext) =>
      plan.profile(queryContext, params)
    })
  }

  @throws(classOf[SyntaxException])
  def profile(query: String, params: JavaMap[String, Any]): ExecutionResult = profile(query, params.asScala.toMap)

  @throws(classOf[SyntaxException])
  def profile(query: String): ExecutionResult = profile(query, Map[String, Any]())


  @throws(classOf[SyntaxException])
  def execute(query: String): ExecutionResult = execute(query, Map[String, Any]())

  @throws(classOf[SyntaxException])
  def execute(query: String, params: Map[String, Any]): ExecutionResult = {
    logger.debug(query)
    prepare(query, { (plan: ExecutionPlan[QueryContext], queryContext: QueryContext) =>
      plan.execute(queryContext, params)
    })
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExecutionResult = execute(query, params.asScala.toMap)

  @throws(classOf[SyntaxException])
  def prepare[T](query: String, run: (ExecutionPlan[QueryContext], QueryContext) => T): T =  {

    var n = 0
    while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
      // create transaction and query context
      var touched = false
      val tx = graph.beginTx()
      val statement = txBridge.statement()
      val plan = try {
        // fetch plan cache
        val planCache = getOrCreateFromSchemaState(statement, new LRUCache[String, ExecutionPlan[QueryContext]](getPlanCacheSize))

        // get plan or build it
        planCache.getOrElseUpdate(query, {
          touched = true
          val planContext = new TransactionBoundPlanContext(statement, graph)
          compiler.prepare(query, planContext)
        })
      }
      catch {
        case (t: Throwable) =>
          statement.close()
          tx.failure()
          tx.close()
          throw t
      }

      if (touched) {
        statement.close()
        tx.success()
        tx.close()
      }
      else {
        val queryContext = executionContext(tx)
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        statement.close()
        return run(plan, queryContext)
      }

      n += 1
    }

    throw new IllegalStateException("Could not execute query due to insanely frequent schema changes")
  }

  private def executionContext(tx: Transaction) =
    new ExceptionTranslatingQueryContext(new TransactionBoundExecutionContext(graph.asInstanceOf[GraphDatabaseAPI], tx, txBridge.statement()))

  private def txBridge = graph.asInstanceOf[GraphDatabaseAPI]
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])

  private def getOrCreateFromSchemaState[V](statement: Statement, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[ExecutionEngine, V]() {
      def apply(key: ExecutionEngine) = creator
    }
    statement.readOperations().schemaStateGetOrCreate(this, javaCreator)
  }

  def prettify(query:String): String = Prettifier(query)

  private def createCorrectCompiler() =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.cypher_parser_version))
      .andThen({
      case v:String => CypherCompiler(graph, v)
      case _        => CypherCompiler(graph)
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => CypherCompiler(graph) )


  private def getPlanCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size))
      .andThen({
      case v: java.lang.Integer => v.intValue()
      case _                    => ExecutionEngine.DEFAULT_PLAN_CACHE_SIZE
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => ExecutionEngine.DEFAULT_PLAN_CACHE_SIZE)

  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}

object ExecutionEngine {
  val DEFAULT_PLAN_CACHE_SIZE: Int = 100
  val PLAN_BUILDING_TRIES: Int = 20
}
