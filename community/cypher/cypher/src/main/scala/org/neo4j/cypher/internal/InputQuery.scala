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

import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Input to query execution
 */
sealed trait InputQuery {
  def options: QueryOptions

  def description: String

  /**
   * Cache key used for executableQueryCache and astCache
   */
  def cacheKey: InputQuery.CacheKey

  def withRecompilationLimitReached: InputQuery

  def withReplanOption(replanOption: CypherReplanOption): InputQuery

}

object InputQuery {

  case class CacheKey(optionsCacheKey: String, statementCacheKey: String) {

    override def toString: String =
      if (optionsCacheKey.isEmpty) statementCacheKey
      else s"${optionsCacheKey} $statementCacheKey"
  }
}

/**
 * Query execution input as a pre-parsed Cypher query.
 */
case class PreParsedQuery(statement: String, rawStatement: String, options: QueryOptions) extends InputQuery {

  override def cacheKey: InputQuery.CacheKey = InputQuery.CacheKey(options.cacheKey, statement)

  def rawPreparserOptions: String = rawStatement.take(rawStatement.length - statement.length)

  override def description: String = rawStatement

  override def withRecompilationLimitReached: PreParsedQuery = copy(options = options.withRecompilationLimitReached)

  override def withReplanOption(replanOption: CypherReplanOption): PreParsedQuery = copy(
    options = options.copy(queryOptions = options.queryOptions.copy(replan = replanOption))
  )
}

/**
 * Query execution input as a fully parsed Cypher query.
 */
case class FullyParsedQuery(state: BaseState, options: QueryOptions) extends InputQuery {

  override lazy val description: String = state.queryText

  override def withRecompilationLimitReached: FullyParsedQuery = copy(options = options.withRecompilationLimitReached)

  override val cacheKey: InputQuery.CacheKey = InputQuery.CacheKey(options.cacheKey, state.queryText)

  override def withReplanOption(replanOption: CypherReplanOption): FullyParsedQuery = copy(
    options = options.copy(queryOptions = options.queryOptions.copy(replan = replanOption))
  )

}

/**
 * Query execution options
 */
case class QueryOptions(offset: InputPosition,
                        isPeriodicCommit: Boolean,
                        queryOptions: CypherQueryOptions,
                        recompilationLimitReached: Boolean = false,
                        materializedEntitiesMode: Boolean = false) {

  def compileWhenHot: Boolean = queryOptions.expressionEngine == CypherExpressionEngineOption.onlyWhenHot || queryOptions.expressionEngine == CypherExpressionEngineOption.default

  def useCompiledExpressions: Boolean = queryOptions.expressionEngine == CypherExpressionEngineOption.compiled || (compileWhenHot && recompilationLimitReached)

  def withRecompilationLimitReached: QueryOptions = copy(recompilationLimitReached = true)

  def withReplanOption(replanOption: CypherReplanOption): QueryOptions = copy(queryOptions = queryOptions.copy(replan = replanOption))

  def withExecutionMode(executionMode: CypherExecutionMode): QueryOptions =
    copy(queryOptions = queryOptions.copy(executionMode = executionMode))

  /**
   * Cache key used for executableQueryCache and astCache.
   */
  def cacheKey: String = {
    val key = queryOptions.cacheKey
    if (key.isBlank) key else "CYPHER " + key
  }

  /**
   * Cache key used for executionPlanCache.
   */
  def executionPlanCacheKey: String = {
    cacheKey + isPeriodicCommit + recompilationLimitReached + materializedEntitiesMode
  }

  /**
   * Cache key used for logicalPlanCache.
   */
  def logicalPlanCacheKey: String = {
    queryOptions.logicalPlanCacheKey
  }

  def render: Option[String] = {
    val text = queryOptions.render
    if (text.isBlank) None else Some("CYPHER " + text)
  }

}

object QueryOptions {
  val default: QueryOptions = QueryOptions(
    offset = InputPosition.NONE,
    isPeriodicCommit = false,
    queryOptions = CypherQueryOptions.default,
  )

}
