/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTExceptionFactory
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.preparser.javacc.CypherPreParser
import org.neo4j.cypher.internal.preparser.javacc.PreParserCharStream
import org.neo4j.cypher.internal.preparser.javacc.PreParserResult
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.exceptions.SyntaxException

import scala.jdk.CollectionConverters.ListHasAsScala

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
 *   statement: 'MATCH (n) RETURN n'
 *   options: QueryOptions(
 *     planner: 'cost'
 *     runtime: 'slotted'
 *     version: '3.5'
 *   )
 * )
 */
class PreParser(
  configuration: CypherConfiguration,
  preParserCache: LFUCache[String, PreParsedQuery]
) {

  /**
   * Clear the pre-parser query cache.
   *
   * @return the number of entries cleared
   */
  def clearCache(): Long = {
    preParserCache.clear()
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
  def preParseQuery(
    queryText: String,
    profile: Boolean = false,
    couldContainSensitiveFields: Boolean = false
  ): PreParsedQuery = {
    val preParsedQuery =
      if (couldContainSensitiveFields) { // This is potentially any outer query running on the system database
        actuallyPreParse(queryText)
      } else {
        preParserCache.computeIfAbsent(queryText, actuallyPreParse(queryText))
      }
    if (profile) {
      preParsedQuery.copy(options = preParsedQuery.options.withExecutionMode(CypherExecutionMode.profile))
    } else {
      preParsedQuery
    }
  }

  private def actuallyPreParse(queryText: String): PreParsedQuery = {
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

    val options = PreParser.queryOptions(
      preParsedStatement.options,
      preParsedStatement.offset,
      configuration
    )

    PreParsedQuery(preParsedStatement.statement, queryText, options)
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
