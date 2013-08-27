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

import internal.commands._
import internal.executionplan.ExecutionPlanBuilder
import internal.executionplan.verifiers.{OptionalPatternWithoutStartVerifier, HintVerifier, Verifier}
import org.neo4j.cypher.internal.{CypherParser, LRUCache}
import org.neo4j.cypher.internal.spi.gdsimpl.{TransactionBoundSchemaQueryContext, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.spi.{SchemaQuery, DataQuery, ExceptionTranslatingQueryContext, QueryContext}
import scala.collection.JavaConverters._
import java.util.{Map => JavaMap}
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI, InternalAbstractGraphDatabase}
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.impl.util.StringLogger
import org.neo4j.cypher.internal.prettifier.Prettifier
import org.neo4j.kernel.api.BaseStatement

class ExecutionEngine(graph: GraphDatabaseService, logger: StringLogger = StringLogger.DEV_NULL) {

  require(graph != null, "Can't work with a null graph database")

  val parser = createCorrectParser()
  val planBuilder = new ExecutionPlanBuilder(graph)
  val verifiers:Seq[Verifier] = Seq(HintVerifier, OptionalPatternWithoutStartVerifier)

  private val queryCache = new LRUCache[String, AbstractQuery](getQueryCacheSize)


  @throws(classOf[SyntaxException])
  def profile(query: String, params: Map[String, Any]): ExecutionResult = {
    logger.debug(query)
    prepare(query, { (plan: ExecutionPlan, queryContext: QueryContext) =>
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
    prepare(query, { (plan: ExecutionPlan, queryContext: QueryContext) =>
      plan.execute(queryContext, params)
    })
  }

  def execute(query: AbstractQuery, params: Map[String, Any]): ExecutionResult = {
    val tx = graph.beginTx()
    try {
      verify(query)
      val plan = executeBaseStatement(stmt => planBuilder.build(new TransactionBoundPlanContext(stmt, graph), query))
      plan.execute(plan.queryType match {
        case DataQuery => createDataQueryContext(tx)
        case SchemaQuery => createSchemaQueryContext(tx)
      }, params)
    }
    catch {
      case (t: Throwable) =>
        tx.failure()
        tx.finish()
        throw t
    }
  }

  @throws(classOf[SyntaxException])
  def execute(query: String, params: JavaMap[String, Any]): ExecutionResult = execute(query, params.asScala.toMap)

  @throws(classOf[SyntaxException])
  def prepare[T](query: String, run: (ExecutionPlan, QueryContext) => T): T =  {
    // parse query
    val cachedQuery: AbstractQuery = queryCache.getOrElseUpdate(query, () => {
      val parsedQuery = parser.parse(query)
      verify(parsedQuery)
      parsedQuery
    })

    var n = 0
    while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {
      // create transaction and query context
      var touched = false
      val tx = graph.beginTx()
      val statement = baseStatement
      val plan = try {
        // fetch plan cache
        val planCache = getOrCreateFromSchemaState(statement, new LRUCache[String, ExecutionPlan](getQueryCacheSize))

        // get plan or build it
        planCache.getOrElseUpdate(query, () => {
          touched = true
          val planContext = new TransactionBoundPlanContext(statement, graph)
          planBuilder.build(planContext, cachedQuery)
        })
      }
      catch {
        case (t: Throwable) =>
          statement.close()
          tx.failure()
          tx.finish()
          throw t
      }

      if (touched) {
        statement.close()
        tx.success()
        tx.finish()
      }
      else {
        val queryContext = plan.queryType match
        {
          case DataQuery => createDataQueryContext(tx)
          case SchemaQuery => createSchemaQueryContext(tx)
        }
        // close the old statement reference after the statement has been "upgraded"
        // to either a schema data or a schema statement, so that the locks are "handed over".
        statement.close()
        return run(plan, queryContext)
      }

      n += 1
    }

    throw new IllegalStateException("Could not execute query due to insanely frequent schema changes")
  }

  private def createDataQueryContext(tx: Transaction) = {
    new ExceptionTranslatingQueryContext(new TransactionBoundQueryContext(graph.asInstanceOf[GraphDatabaseAPI], tx, dataStatement))
  }

  private def createSchemaQueryContext(tx: Transaction) = {
    new ExceptionTranslatingQueryContext(new TransactionBoundSchemaQueryContext(graph.asInstanceOf[GraphDatabaseAPI], tx, schemaStatement))
  }

  private def dataStatement = txBridge.dataStatement()
  private def schemaStatement = txBridge.schemaStatement()
  private def baseStatement = txBridge.baseStatement()
  private def executeBaseStatement[T](callback: (BaseStatement) => T): T = {
    val stmt = baseStatement
    try {
      callback( stmt )
    } finally {
      stmt.close()
    }
  }

  private def txBridge = graph.asInstanceOf[GraphDatabaseAPI]
    .getDependencyResolver
    .resolveDependency(classOf[ThreadToStatementContextBridge])

  private def getOrCreateFromSchemaState[V](statement: BaseStatement, creator: => V) = {
    val javaCreator = new org.neo4j.helpers.Function[ExecutionEngine, V]() {
      def apply(key: ExecutionEngine) = creator
    }
    statement.schemaStateGetOrCreate(this, javaCreator)
  }

  def verify(query: AbstractQuery) {
    for ( verifier <- verifiers )
    {
      verifier.verify(query)
    }
  }

  def prettify(query:String): String = Prettifier(query)

  private def createCorrectParser() =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.cypher_parser_version))
      .andThen({
      case v:String => CypherParser(v)
      case _        => CypherParser()
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => CypherParser() )


  private def getQueryCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size))
      .andThen({
      case v: java.lang.Integer => v.intValue()
      case _                    => ExecutionEngine.DEFAULT_QUERY_CACHE_SIZE
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => ExecutionEngine.DEFAULT_QUERY_CACHE_SIZE)

  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}

object ExecutionEngine {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 100
  val PLAN_BUILDING_TRIES: Int = 20
}



