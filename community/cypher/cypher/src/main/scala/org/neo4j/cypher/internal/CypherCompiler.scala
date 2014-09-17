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

case class PreParsedQuery(statement: String, version: CypherVersion, planType: PlanType, plannerVersion: PlannerVersion)


class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion = CypherVersion.vDefault,
                     optionParser: CypherOptionParser) {
  private val queryCacheSize: Int = getQueryCacheSize
  private val compatibilityFor1_9 = CompatibilityFor1_9(graph, queryCacheSize)
  private val compatibilityFor2_0 = CompatibilityFor2_0(graph, queryCacheSize)
  private val compatibilityFor2_1 = CompatibilityFor2_1(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Rule = CompatibilityFor2_2Rule(graph, queryCacheSize, kernelMonitors, kernelAPI)
  private val compatibilityFor2_2Cost = CompatibilityFor2_2Cost(graph, queryCacheSize, kernelMonitors, kernelAPI)

  @throws(classOf[SyntaxException])
  def parseQuery(queryText: String): ParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery: PreParsedQuery = preParse(queryWithOptions)
    val planType = preParsedQuery.planType
    val version = preParsedQuery.version
    val statementAsText = preParsedQuery.statement
    val plannerVersion = preParsedQuery.plannerVersion

    (version, plannerVersion) match {
      case (CypherVersion.v2_2, PlannerVersion.costPlanner) => compatibilityFor2_2Cost.produceParsedQuery(statementAsText, planType)
      case (CypherVersion.v2_2, PlannerVersion.rulePlanner) => compatibilityFor2_2Rule.produceParsedQuery(statementAsText, planType)
      case (CypherVersion.v2_2, _)    => compatibilityFor2_2Cost.produceParsedQuery(statementAsText, planType)
      case (CypherVersion.v2_1, _)    => compatibilityFor2_1.parseQuery(statementAsText, planType == Profiled)
      case (CypherVersion.v2_0, _)    => compatibilityFor2_0.parseQuery(statementAsText, planType == Profiled)
      case (CypherVersion.v1_9, _)    => compatibilityFor1_9.parseQuery(statementAsText, planType == Profiled)
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions): PreParsedQuery = {

    import org.neo4j.cypher.internal.CollectionFrosting._

    val versionOptions = queryWithOption.options.collectSingle {
      case VersionOption(v) => CypherVersion(v)
    }

    val cypherVersion = versionOptions match {
      case Right(version) => version.getOrElse(defaultVersion)
      case Left(versions) => throw new SyntaxException(s"You must specify only one version for a query (found: $versions)")
    }

    val planType: PlanType = calculatePlanType(queryWithOption.options)

    if (planType == Explained && cypherVersion != CypherVersion.v2_2) {
      throw new InvalidArgumentException("EXPLAIN not supported in versions older than Neo4j v2.2")
    }

    val plannerVersion = calculatePlanerVersion(queryWithOption.options)

    PreParsedQuery(queryWithOption.statement, cypherVersion, planType, plannerVersion)
  }

  private def calculatePlanType(options: Seq[CypherOption]) = {
    val planTypes: Seq[PlanType] = options.collect {
      case ExplainOption => Explained
      case ProfileOption => Profiled
    }

    planTypes.reduceOption(_ combineWith _).getOrElse(Normal)
  }

  private def calculatePlanerVersion(options: Seq[CypherOption]) = {
    val planner = options.collect {
      case CostPlanner => PlannerVersion.costPlanner
      case RulePlanner => PlannerVersion.rulePlanner
    }.distinct

    if (planner.size > 1) {
      throw new InvalidSemanticsException("Can't use multiple planners")
    }

    if (planner.isEmpty) PlannerVersion.default else planner.head
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
