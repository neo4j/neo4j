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

import spi.gdsimpl.TransactionBoundPlanContext
import org.neo4j.cypher._
import CypherVersion._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.cypher.internal.spi.QueryContext

trait CypherCompiler {
  def prepare(query: String, context: TransactionBoundPlanContext): ExecutionPlan[QueryContext]
}


object CypherCompiler {
  def apply(graph: GraphDatabaseService) = VersionProxy(graph, CypherVersion.vDefault)
  def apply(graph: GraphDatabaseService, versionName: String) = VersionProxy(graph, CypherVersion(versionName))
  def apply(graph: GraphDatabaseService, defaultVersion: CypherVersion) = VersionProxy(graph, defaultVersion)

  private val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r

  val DEFAULT_QUERY_CACHE_SIZE: Int = 100

  case class VersionProxy(graph: GraphDatabaseService, defaultVersion: CypherVersion) extends CypherCompiler {
    private val queryCache = new LRUCache[(CypherVersion, String), Object](getQueryCacheSize)

    private val compilers = Map(
      v1_9 -> new compiler.v1_9.CypherCompiler(graph, (q, f) => queryCache.getOrElseUpdate((v1_9, q), f)),
      v2_0 -> new compiler.v2_0.CypherCompiler(graph, (q, f) => queryCache.getOrElseUpdate((v2_0, q), f))
    )

    @throws(classOf[SyntaxException])
    def prepare(query: String, context: TransactionBoundPlanContext): ExecutionPlan[QueryContext] = {
      val (version, remainingQuery) = query match {
        case hasVersionDefined(versionName, remainingQuery) => (CypherVersion(versionName), remainingQuery)
        case _ => (vDefault, query)
      }
      compilers(version).prepare(remainingQuery, context)
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
