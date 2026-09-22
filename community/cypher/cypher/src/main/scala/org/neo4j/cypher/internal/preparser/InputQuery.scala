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
package org.neo4j.cypher.internal.preparser

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.helpers.QueryLanguageConverter
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.notification.DeprecatedRuntimeNotification
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.options.CypherDerivedQueryOptions
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherParallelRuntimeSupportOption
import org.neo4j.cypher.internal.options.CypherPlanMode
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersionOption
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Input to query execution
 */
sealed trait InputQuery {
  def options: QueryOptions
  final def resolvedLanguage: CypherVersion = options.resolvedLanguage
  def description: String

  /**
   * Cache key used for executableQueryCache and astCache
   */
  def cacheKey: InputQuery.CacheKey

  def withRecompilationLimitReached: InputQuery

  def withReplanOption(replanOption: CypherReplanOption): InputQuery

  def notifications: Seq[InternalNotification] = options.queryOptions.runtime match {
    case CypherRuntimeOption.interpreted =>
      Seq(DeprecatedRuntimeNotification(
        "'runtime=interpreted' is deprecated, please use 'runtime=slotted' instead",
        "runtime=interpreted",
        "runtime=slotted"
      ))
    case _ => Seq.empty
  }

  def workingScopeOpt: Option[WorkingScope] = None
}

object InputQuery {

  case class CacheKey(optionsCacheKey: String, statementCacheKey: String, resolvedLanguage: CypherVersion) {

    override def toString: String =
      if (optionsCacheKey.isEmpty) statementCacheKey
      else s"${optionsCacheKey} $statementCacheKey"
  }
}

/**
 * Query execution input as a pre-parsed Cypher query.
 */
case class PreParsedQuery(
  statement: String,
  rawStatement: String,
  options: QueryOptions,
  additionalNotifications: Seq[InternalNotification] = Seq.empty
) extends InputQuery {

  override def cacheKey: InputQuery.CacheKey = InputQuery.CacheKey(options.cacheKey, statement, resolvedLanguage)
  def preParserCacheKey: PreParsedQuery.CacheKey = PreParsedQuery.CacheKey(rawStatement, options.defaultLanguage)

  def cacheKeyWithRawStatement: InputQuery.CacheKey =
    InputQuery.CacheKey(options.cacheKey, rawStatement, resolvedLanguage)

  def rawPreparserOptions: String = rawStatement.take(rawStatement.length - statement.length)

  override def description: String = rawStatement

  override def withRecompilationLimitReached: PreParsedQuery = copy(options = options.withRecompilationLimitReached)

  override def withReplanOption(replanOption: CypherReplanOption): PreParsedQuery = copy(
    options = options.copy(queryOptions = options.queryOptions.copy(replan = replanOption))
  )

  override def notifications: Seq[InternalNotification] = super.notifications ++ additionalNotifications
}

object PreParsedQuery {

  /**
   * @param statement statement, including preparser options
   * @param defaultLanguage the db default version, NOT the actual version to use for this query
   */
  case class CacheKey(statement: String, defaultLanguage: CypherVersion)
}

/**
 * Query execution input as a fully parsed Cypher query.
 */
case class FullyParsedQuery(state: BaseState, options: QueryOptions) extends InputQuery {

  override lazy val description: String = state.queryText

  override def withRecompilationLimitReached: FullyParsedQuery = copy(options = options.withRecompilationLimitReached)

  override val cacheKey: InputQuery.CacheKey = InputQuery.CacheKey(options.cacheKey, state.queryText, resolvedLanguage)

  override def withReplanOption(replanOption: CypherReplanOption): FullyParsedQuery = copy(
    options = options.copy(queryOptions = options.queryOptions.copy(replan = replanOption))
  )

  override def workingScopeOpt: Option[WorkingScope] = state.maybeScopeState.map(_.workingScope)
}

/**
 * Query execution options
 */
case class QueryOptions(
  offset: InputPosition,
  queryOptions: CypherQueryOptions,
  derivedOptions: CypherDerivedQueryOptions,
  recompilationLimitReached: Boolean = false,
  materializedEntitiesMode: Boolean = false,
  private[preparser] val defaultLanguage: CypherVersion // The db default language (NOT the version to use)
) {
  val resolvedLanguage: CypherVersion = queryOptions.cypherVersion.explicitVersion.getOrElse(defaultLanguage)

  def compileWhenHot: Boolean =
    queryOptions.expressionEngine == CypherExpressionEngineOption.onlyWhenHot || queryOptions.expressionEngine == CypherExpressionEngineOption.default

  def useCompiledExpressions: Boolean =
    queryOptions.expressionEngine == CypherExpressionEngineOption.compiled || (compileWhenHot && recompilationLimitReached)

  def withRecompilationLimitReached: QueryOptions = copy(recompilationLimitReached = true)

  def withMaterializedEntitiesMode(mode: Boolean): QueryOptions = copy(materializedEntitiesMode = mode)

  def withReplanOption(replanOption: CypherReplanOption): QueryOptions =
    copy(queryOptions = queryOptions.copy(replan = replanOption))

  def withExecutionMode(executionMode: CypherExecutionMode): QueryOptions =
    copy(queryOptions = queryOptions.copy(executionMode = executionMode))

  def withPlanMode(planMode: CypherPlanMode): QueryOptions =
    copy(queryOptions = queryOptions.copy(planMode = planMode))

  def withQueryLanguage(language: CypherVersion): QueryOptions =
    copy(queryOptions =
      queryOptions.copy(cypherVersion = CypherVersionOption.values.find(_.explicitVersion.contains(language)).get)
    )

  def withInferSchemaPartsOption(inferSchemaPartsOption: CypherInferSchemaPartsOption): QueryOptions =
    copy(queryOptions =
      queryOptions.copy(inferSchemaParts = inferSchemaPartsOption)
    )

  /**
   * Cache key used for executableQueryCache and astCache.
   *
   * Even though the materializedEntitiesMode does influence planning,
   * it is not included in the cache key for 2 reasons.
   * - The option is only true iff running a Fabric query and a fabric query cannot equal a non-Fabric query.
   * - It would break users of this method since this is expected to be a String that can be parsed back by the pre-parser.
   */
  def cacheKey: String = {
    val key = queryOptions.cacheKey
    val derivedKey = derivedOptions.cacheKey
    val combinedKey = key + derivedKey
    if (key.isBlank) combinedKey else "CYPHER " + combinedKey
  }

  /**
   * Cache key used for executionPlanCache.
   */
  def executionPlanCacheKey: String = {
    cacheKey + recompilationLimitReached + materializedEntitiesMode
  }

  /**
   * Cache key used for logicalPlanCache.
   */
  def logicalPlanCacheKey: String = {
    queryOptions.logicalPlanCacheKey + derivedOptions.logicalPlanCacheKey
  }

  def render: Option[String] = {
    val options = queryOptions.renderCypherOptions
    val cypherOptions = if (!options.isBlank) "CYPHER " + options else ""
    val executionOptions = (queryOptions.renderExecutionMode + " " + queryOptions.renderPlanMode).trim
    val fullOptions = (executionOptions + " " + cypherOptions).trim
    if (fullOptions.isBlank) None else Some(fullOptions)
  }

}

object QueryOptions {

  def default(cypherConfig: CypherConfiguration, defaultLanguage: CypherVersion): QueryOptions = QueryOptions(
    offset = InputPosition.NONE,
    queryOptions = CypherQueryOptions.defaultOptions,
    derivedOptions = CypherQueryOptions.derivedOptions(CypherQueryOptions.defaultOptions, cypherConfig),
    defaultLanguage = defaultLanguage
  )

  // Used by micro benchmarks
  def defaultWith(
    defaultLanguage: CypherVersion,
    useCompiledExpressions: Boolean,
    materializedEntitiesMode: Boolean,
    operatorEngine: CypherOperatorEngineOption,
    parallelRuntimeSupport: CypherParallelRuntimeSupportOption
  ): QueryOptions = {
    val expressionEngine =
      if (useCompiledExpressions) CypherExpressionEngineOption.compiled else CypherExpressionEngineOption.interpreted
    QueryOptions(
      offset = InputPosition.NONE,
      queryOptions = CypherQueryOptions.defaultOptions.copy(
        expressionEngine = expressionEngine,
        operatorEngine = operatorEngine,
        parallelRuntimeSupportOption = parallelRuntimeSupport
      ),
      derivedOptions = CypherQueryOptions.defaultDerivedOptions,
      materializedEntitiesMode = materializedEntitiesMode,
      defaultLanguage = defaultLanguage
    )
  }

  // Test-only
  def default(defaultLanguage: CypherVersion): QueryOptions = {
    QueryOptions(
      offset = InputPosition.NONE,
      queryOptions = CypherQueryOptions.defaultOptions,
      derivedOptions = CypherQueryOptions.defaultDerivedOptions,
      defaultLanguage = defaultLanguage
    )
  }

  // Test-only (called from Java, which doesn't like default() as a method name)
  def defaultJava(defaultLanguage: CypherVersion): QueryOptions = {
    default(defaultLanguage)
  }

  // Test-only
  def defaultDefault(): QueryOptions = {
    default(QueryLanguageConverter.toInternal(GraphDatabaseSettings.default_language.defaultValue()))
  }
}
