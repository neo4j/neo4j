/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.preparser.javacc.CypherPreParser
import org.neo4j.cypher.internal.preparser.javacc.PreParserCharStream
import org.neo4j.cypher.internal.preparser.javacc.PreParserResult
import org.neo4j.cypher.internal.util.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.exceptions.SyntaxException

import java.util.Locale

import scala.jdk.CollectionConverters.ListHasAsScala

/**
 * Preparses Cypher queries.
 *
 * The PreParser converts queries like
 *
 * 'CYPHER planner=cost,runtime=slotted MATCH (n) RETURN n'
 *
 * into
 *
 * PreParsedQuery(
 *   statement: 'MATCH (n) RETURN n'
 *   options: QueryOptions(
 *     planner: 'cost'
 *     runtime: 'slotted'
 *   )
 * )
 */
class CachingPreParser(
  configuration: CypherConfiguration,
  preParserCache: LFUCache[String, PreParsedQuery]
) extends PreParser(configuration) {

  /**
   * Clear the pre-parser query cache.
   *
   * @return the number of entries cleared
   */
  def clearCache(): Long = {
    preParserCache.clear()
  }

  def insertIntoCache(queryText: String, preParsedQuery: PreParsedQuery): Unit = {
    preParserCache.put(queryText, preParsedQuery)
  }

  /**
   * Pre-parse a user-specified cypher query.
   *
   * @param queryText                   the query
   * @param notificationLogger          records notifications during pre parsing
   * @param profile                     true if the query should be profiled even if profile is not given as a pre-parser option
   * @param couldContainSensitiveFields true if the query might contain passwords, like some administrative commands can
   * @param targetsComposite            true if the query targets a composite database
   * @throws SyntaxException if there are syntactic errors in the pre-parser options
   * @return the pre-parsed query
   */
  @throws(classOf[SyntaxException])
  def preParseQuery(
    queryText: String,
    notificationLogger: InternalNotificationLogger,
    profile: Boolean = false,
    couldContainSensitiveFields: Boolean = false,
    targetsComposite: Boolean = false
  ): PreParsedQuery = {
    val preParsedQuery =
      if (couldContainSensitiveFields) { // This is potentially any outer query running on the system database
        preParse(queryText, notificationLogger)
      } else {
        preParserCache.computeIfAbsent(queryText, preParse(queryText, notificationLogger))
      }
    preParsedQuery.notifications.foreach(notificationLogger.log)
    if (profile) {
      preParsedQuery.copy(options = preParsedQuery.options.withExecutionMode(CypherExecutionMode.profile))
    } else if (targetsComposite) {
      preParsedQuery.copy(options =
        preParsedQuery.options.copy(
          queryOptions = QueryOptions.default.queryOptions.copy(
            runtime = CypherRuntimeOption.slotted,
            expressionEngine = CypherExpressionEngineOption.interpreted
          ),
          materializedEntitiesMode = true
        )
      )
    } else {
      preParsedQuery
    }
  }
}

class PreParser(
  configuration: CypherConfiguration
) {

  def preParse(
    queryText: String,
    notificationLogger: InternalNotificationLogger
  ): PreParsedQuery = {
    val exceptionFactory = new Neo4jASTExceptionFactory(Neo4jCypherExceptionFactory(queryText, None))
    if (queryText.isEmpty) {
      throw exceptionFactory.syntaxException(
        new IllegalStateException(PreParserResult.getEmptyQueryExceptionMsg),
        1,
        0,
        0
      )
    }
    val preParserResult = new CypherPreParser(exceptionFactory, new PreParserCharStream(queryText)).parse()
    val preParsedStatement = PreParsedStatement(
      queryText.substring(preParserResult.position.offset),
      preParserResult.options.asScala.toList,
      preParserResult.position
    )

    val notifications = preParsedStatement.options.collect {
      case PreParserOption(key, _, pos) if key.toLowerCase(Locale.ROOT) == CypherConnectComponentsPlannerOption.key =>
        DeprecatedConnectComponentsPlannerPreParserOption(pos)
    }

    val options = PreParser.queryOptions(
      preParsedStatement.options,
      preParsedStatement.offset,
      configuration
    )

    PreParsedQuery(preParsedStatement.statement, queryText, options, notifications)
  }

}

object PreParser {

  def queryOptions(
    preParsedOptions: List[PreParserOption],
    offset: InputPosition,
    configuration: CypherConfiguration
  ): QueryOptions = {

    val preParsedOptionsSet = preParsedOptions.map(o => (o.key, o.value)).toSet

    val options = CypherQueryOptions.fromValues(configuration, preParsedOptionsSet)

    QueryOptions(
      offset,
      options
    )
  }
}
