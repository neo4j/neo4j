/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.options.InvalidCypherOption
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.SyntaxException

import scala.util.matching.Regex

/**
 * Preparses Cypher queries.
 *
 * The PreParser converts queries like
 *
 * 'CYPHER 3.5 planner=cost,runtime=slotted MATCH (n) RETURN n'
 *
 * into
 *
 * PreParsedQuery(
 * statement: 'MATCH (n) RETURN n'
 * options: QueryOptions(
 * planner: 'cost'
 * runtime: 'slotted'
 * version: '3.5'
 * )
 * )
 */
class PreParser(
  configuration: CypherConfiguration,
  planCacheSize: Int,
  cacheFactory: CaffeineCacheFactory) {

  def this(configuration: CypherConfiguration, cacheFactory: CaffeineCacheFactory) =
    this(configuration, configuration.queryCacheSize, cacheFactory)

  private val preParsedQueries = new LFUCache[String, PreParsedQuery](cacheFactory, planCacheSize)

  /**
   * Clear the pre-parser query cache.
   *
   * @return the number of entries cleared
   */
  def clearCache(): Long = {
    preParsedQueries.clear()
  }

  /**
   * Pre-parse a user-specified cypher query.
   *
   * @param queryText                   the query
   * @param profile                     true if the query should be profiled even if profile is not given as a pre-parser option
   * @param couldContainSensitiveFields true if the query might contain passwords, like some administrative commands can
   * @throws SyntaxException if there are syntactic errors in the pre-parser options
   * @return the pre-parsed query
   */
  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String, profile: Boolean = false, couldContainSensitiveFields: Boolean = false): PreParsedQuery = {
    val preParsedQuery = if (couldContainSensitiveFields) { // This is potentially any outer query running on the system database
      actuallyPreParse(queryText)
    } else {
      preParsedQueries.computeIfAbsent(queryText, actuallyPreParse(queryText))
    }
    if (profile) {
      preParsedQuery.copy(options = preParsedQuery.options.copy(executionMode = CypherExecutionMode.profile))
    } else {
      preParsedQuery
    }
  }

  private def actuallyPreParse(queryText: String): PreParsedQuery = {
    val preParsedStatement = CypherPreParser(queryText)
    val isPeriodicCommit = PreParser.periodicCommitHintRegex.findFirstIn(preParsedStatement.statement.toUpperCase).nonEmpty

    val options = PreParser.queryOptions(
      preParsedStatement.options,
      preParsedStatement.offset,
      isPeriodicCommit,
      configuration,
    )

    PreParsedQuery(preParsedStatement.statement, queryText, options)
  }
}

object PreParser {
  val periodicCommitHintRegex: Regex = "^\\s*USING\\s+PERIODIC\\s+COMMIT.*".r

  def queryOptions(
    preParsedOptions: List[PreParserOption],
    offset: InputPosition,
    isPeriodicCommit: Boolean,
    configuration: CypherConfiguration): QueryOptions = {

    val preParsedOptionsSet = preParsedOptions.toSet

    val versions = preParsedOptionsSet.collect { case option: VersionPreParserOption => option.value }
    val executionModes = preParsedOptionsSet.collect { case option: ModePreParserOption => option.value.name }
    val keyValues = preParsedOptionsSet.collect { case option: KeyValuePreParserOption => option.key -> option.value }

    val options = CypherQueryOptions.fromValues(configuration, executionModes, versions, keyValues)

    QueryOptions(
      offset,
      isPeriodicCommit,
      options.version,
      options.executionMode,
      options.planner,
      options.runtime,
      options.updateStrategy,
      options.expressionEngine,
      options.operatorEngine,
      options.interpretedPipesFallback,
      options.replan,
      options.connectComponentsPlanner,
      options.debugOptions,
    )
  }
}
