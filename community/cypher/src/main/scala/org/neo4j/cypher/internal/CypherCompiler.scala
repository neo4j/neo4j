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
package org.neo4j.cypher.internal

import org.neo4j.cypher._
import CypherVersion._
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.kernel.{GraphDatabaseAPI, InternalAbstractGraphDatabase}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.cypher.internal.compiler.v2_0.{CypherCompiler => CypherCompiler2_0}
import org.neo4j.cypher.internal.compiler.v1_9.{CypherCompiler => CypherCompiler1_9}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlan => ExecutionPlan_v2_0}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlan => ExecutionPlan_v1_9}
import org.neo4j.kernel.api.Statement
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundExecutionContext, TransactionBoundPlanContext}
import org.neo4j.cypher.internal.compiler.v2_0.spi.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.spi.v1_9.GDSBackedQueryContext

object CypherCompiler {
  def apply(graph: GraphDatabaseService) = VersionProxy(graph, CypherVersion.vDefault)
  def apply(graph: GraphDatabaseService, versionName: String) = VersionProxy(graph, CypherVersion(versionName))
  def apply(graph: GraphDatabaseService, defaultVersion: CypherVersion) = VersionProxy(graph, defaultVersion)

  private val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

  val DEFAULT_QUERY_CACHE_SIZE: Int = 100

  case class VersionProxy(graph: GraphDatabaseService, defaultVersion: CypherVersion) {
    private val queryCache = new LRUCache[(CypherVersion, String), Object](getQueryCacheSize)
    private val compiler2_0 = new CypherCompiler2_0(graph, (q, f) => queryCache.getOrElseUpdate((v2_0, q), f))
    private val compiler1_9 = new CypherCompiler1_9(graph, (q, f) => queryCache.getOrElseUpdate((v2_0, q), f))


    @throws(classOf[SyntaxException])
    def prepare(query: String, context: GraphDatabaseService, statement: Statement): ExecutionPlan = {
      val (version, remainingQuery) = query match {
        case hasVersionDefined(versionName, remainingQuery) => (CypherVersion(versionName), remainingQuery)
        case _ => (vDefault, query)
      }

      version match {
        case CypherVersion.v1_9 =>
          val plan = compiler1_9.prepare(remainingQuery)
          new ExecutionPlanWrapperForV1_9(plan)

        case CypherVersion.v2_0 => 
          val plan = compiler2_0.prepare(remainingQuery, new TransactionBoundPlanContext(statement, context))
          new ExecutionPlanWrapperForV2_0(plan)
      }
    }

    private def getQueryCacheSize : Int =
      optGraphAs[InternalAbstractGraphDatabase]
        .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size))
        .andThen({
        case v: java.lang.Integer => v.intValue()
        case _                    => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE
      })
        .applyOrElse(graph, (_: GraphDatabaseService) => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE)
  }

  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}

class ExecutionPlanWrapperForV2_0(inner: ExecutionPlan_v2_0) extends ExecutionPlan {

  private def executionContext(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement) =
    new ExceptionTranslatingQueryContext(new TransactionBoundExecutionContext(graph, tx, statement))

  def profile(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement, params: Map[String, Any]) =
    inner.profile(executionContext(graph, tx, statement), params)

  def execute(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement, params: Map[String, Any]) =
    inner.execute(executionContext(graph, tx, statement), params)
}

class ExecutionPlanWrapperForV1_9(inner: ExecutionPlan_v1_9) extends ExecutionPlan {

  private def executionContext(graph: GraphDatabaseAPI) =
    new GDSBackedQueryContext(graph)

  def profile(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement, params: Map[String, Any]) =
    inner.profile(executionContext(graph), tx, params)

  def execute(graph: GraphDatabaseAPI, tx: Transaction, statement: Statement, params: Map[String, Any]) =
    inner.execute(executionContext(graph), tx, params)
}

