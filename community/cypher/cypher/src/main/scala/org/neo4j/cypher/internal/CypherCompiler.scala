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
package org.neo4j.cypher.internal

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatability._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.InternalAbstractGraphDatabase
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
}

case class PreParsedQuery(statement: String, version: CypherVersion, planType: PlanType)


class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion = CypherVersion.vDefault,
                     optionParser: CypherOptionParser) {
  private val queryCacheSize: Int = getQueryCacheSize
  private val compatibilityFor1_9 = CompatibilityFor1_9(graph, queryCacheSize)
  private val compatibilityFor2_0 = CompatibilityFor2_0(graph, queryCacheSize)
  private val compatibilityFor2_1 = CompatibilityFor2_1(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Legacy = CompatibilityFor2_2Legacy(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Experimental = CompatibilityFor2_2Experimental(graph, queryCacheSize, kernelMonitors, kernelAPI)

  @throws(classOf[SyntaxException])
  def parseQuery(queryText: String): ParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery: PreParsedQuery = preParse(queryWithOptions)
    val planType = preParsedQuery.planType
    val version = preParsedQuery.version
    val statementAsText = preParsedQuery.statement

    version match {
      case CypherVersion.experimental => compatibilityFor2_2Experimental.produceParsedQuery(statementAsText, planType)
      case CypherVersion.v2_2 => compatibilityFor2_2Legacy.produceParsedQuery(statementAsText, planType)
      case CypherVersion.v2_1 if planType == Normal => compatibilityFor2_1.parseQuery(statementAsText)
      case CypherVersion.v2_0 => compatibilityFor2_0.parseQuery(statementAsText)
      case CypherVersion.v1_9 => compatibilityFor1_9.parseQuery(statementAsText)
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions): PreParsedQuery = {

    import CollectionFrosting._

    val versionOptions = queryWithOption.options.collectSingle({ case VersionOption( v ) => v })
    val version = versionOptions match {
      case Right(Some(v)) => CypherVersion(v)
      case Right(None)    => defaultVersion
      case Left(versions) => throw new SyntaxException(s"You must specify only one version for a query (found: $versions)")
    }

    val planType: PlanType = queryWithOption.options.collectFirst {
      case ExplainOption => Explained
    }.getOrElse(Normal)

    PreParsedQuery(queryWithOption.statement, version, planType)
  }

  private def getQueryCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size))
      .andThen({
      case v: java.lang.Integer => v.intValue()
      case _                    => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE)

  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}
