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

import org.antlr.v4.runtime.BailErrorStrategy
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.InputMismatchException
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.neo4j.cypher.internal.PreParser.queryOptions
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.notification.DeprecatedConnectComponentsPlannerPreParserOption
import org.neo4j.cypher.internal.notification.DeprecatedEagerAnalyzerPreParserOption
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.notification.RetiredPlannerVersionPreParserOption
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherEagerAnalyzerOption
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherPlanMode
import org.neo4j.cypher.internal.options.CypherPlannerVersionOption
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersionOption
import org.neo4j.cypher.internal.parser.SyntaxErrorListener
import org.neo4j.cypher.internal.parser.lexer.UnicodeEscapeReplacementReader.InvalidUnicodeLiteral
import org.neo4j.cypher.internal.preparser.CypherPreparserParser
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.preparser.PreParsedStatement
import org.neo4j.cypher.internal.preparser.PreparserCypherLexer
import org.neo4j.cypher.internal.preparser.PreparserUtil
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.preparser.StatefulPreparserListener
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.exceptions.InvalidCypherOption
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlHelper

import java.util.Locale

/**
 * Preparses Cypher queries.
 *
 * The PreParser converts queries like
 *
 * 'CYPHER planner=cost runtime=slotted MATCH (n) RETURN n'
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
  preParserCache: LFUCache[PreParsedQuery.CacheKey, PreParsedQuery]
) extends PreParser(configuration) {

  /**
   * Clear the pre-parser query cache.
   *
   * @return the number of entries cleared
   */
  def clearCache(): Long = {
    PreparserUtil.clearDFACache()
    preParserCache.clear()
  }

  def insertIntoCache(preParsedQuery: PreParsedQuery): Unit = {
    preParserCache.put(preParsedQuery.preParserCacheKey, preParsedQuery)
  }

  /**
   * Pre-parse a user-specified cypher query.
   *
   * @param queryText                   the query
   * @param notificationLogger          records notifications during pre parsing
   * @param defaultLanguage             the database specific default query language
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
    defaultLanguage: CypherVersion,
    profile: Boolean = false,
    couldContainSensitiveFields: Boolean = false,
    targetsComposite: Boolean = false,
    cacheStrategy: CacheStrategy = CacheStrategy.defaultDefault
  ): PreParsedQuery = {
    val preParsedQuery =
      if (couldContainSensitiveFields || !cacheStrategy.preParserShouldBeCached) { // This is potentially any outer query running on the system database
        val ppq = preParse(queryText, defaultLanguage)
        // Check again if the pre-parsed query options demand that we cache it
        // NOTE: cache=force overrides couldContainSensitiveFields
        if (cacheStrategy.updateFromQueryOptions(ppq.options.queryOptions).preParserShouldBeCached) {
          preParserCache.put(ppq.preParserCacheKey, ppq)
        }
        ppq
      } else {
        val key = PreParsedQuery.CacheKey(queryText, defaultLanguage)
        var ppq: PreParsedQuery = null
        val cachedPpq = preParserCache.computeIfAbsent(
          key, {
            ppq = preParse(queryText, defaultLanguage)
            if (cacheStrategy.updateFromQueryOptions(ppq.options.queryOptions).preParserShouldBeCached) {
              ppq
            } else {
              // The pre-parsed query options demand that we do not cache this query
              null
            }
          }
        )
        if (cachedPpq != null) {
          cachedPpq
        } else {
          ppq // Always return the pre-parsed query even if we decided not to cache it
        }
      }
    preParsedQuery.notifications.foreach(notificationLogger.log)
    if (profile) {
      preParsedQuery.copy(options =
        preParsedQuery.options.copy(
          queryOptions = preParsedQuery.options.queryOptions.copy(
            executionMode = CypherExecutionMode.profile,
            planMode = CypherPlanMode.default
          )
        )
      )
    } else if (targetsComposite) {
      preParsedQuery.copy(options =
        preParsedQuery.options.copy(
          queryOptions = preParsedQuery.options.queryOptions.copy(
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

  private val EMPTY_QUERY_PARSER_EXCEPTION_MSG = "Unexpected end of input: expected CYPHER, EXPLAIN, PROFILE or Query"

  @deprecated("To be removed soon", since = "01/2025")
  def preParse(query: String, notifications: InternalNotificationLogger): PreParsedQuery =
    preParse(query, configuration.systemDefaultLanguage)

  @deprecated("To be removed soon", since = "01/2025")
  def preParse(query: String): PreParsedQuery = preParse(query, configuration.systemDefaultLanguage)

  /**
   * Pre-parse query.
   *
   * @param query the query
   * @param defaultLanguage the database specific default query language
   * @return
   */
  def preParse(query: String, defaultLanguage: CypherVersion): PreParsedQuery = {
    val preParsedStatement = preParseQuery(query)
    val notifications = preParserOptionsNotifications(preParsedStatement)
    val options = queryOptions(preParsedStatement, configuration, defaultLanguage)
    val preparsed = PreParsedQuery(preParsedStatement.statement, query, options, notifications)
    if (preparsed.resolvedLanguage.experimental && !configuration.enableExperimentalCypherVersions) {
      throw InvalidCypherOption.invalidOption(
        preparsed.resolvedLanguage.versionName,
        CypherVersionOption.name,
        CypherVersionOption.supportedValues.map(_.name): _*
      )
    }
    preparsed
  }

  private def preParseQuery(queryText: String): PreParsedStatement = {
    val exceptionFactory = Neo4jCypherExceptionFactory(queryText, None)

    val tokenStream =
      try {
        val lexer =
          PreparserCypherLexer.fromString(queryText, false)
        lexer.removeErrorListeners()
        lexer.addErrorListener(new SyntaxErrorListener(exceptionFactory))
        new CommonTokenStream(lexer)
      } catch {
        case e: InvalidUnicodeLiteral =>
          throw exceptionFactory.syntaxException(
            GqlHelper.getGql42001_42I47(e.getMessage, e.offset, e.line, e.column),
            e.getMessage,
            InputPosition(e.offset, e.line, e.column)
          )
      }

    val (queryStart, settings) = if (hasPreparserOptions(tokenStream.LA(1))) {
      val preparser = new CypherPreparserParser(tokenStream)
      val statefulPreparserListener = new StatefulPreparserListener()
      preparser.addParseListener(statefulPreparserListener)
      preparser.setErrorHandler(new BailErrorStrategy)
      preparser.removeErrorListeners()

      try {
        preparser.preparserOptions()
      } catch {
        case ex: ParseCancellationException =>
          ex.getCause match {
            case exx: InputMismatchException =>
              exx.getCtx match {
                case ctx: CypherPreparserParser.SettingContext if !ctx.IDENTIFIER.isEmpty =>
                  throw InvalidCypherOption.unsupportedOptions(ctx.IDENTIFIER(0).getText)
                case _ =>
                  throw InvalidCypherOption.unsupportedOptions("")
              }
            case _ =>
              throw InvalidCypherOption.unsupportedOptions("")
          }
      }

      if (statefulPreparserListener.queryPosition.isEmpty) {
        throw exceptionFactory.syntaxException(
          GqlHelper.getGql42001_42N45(0, 1, 1),
          EMPTY_QUERY_PARSER_EXCEPTION_MSG,
          InputPosition(0, 1, 1)
        )
      }

      (statefulPreparserListener.queryPosition.get, statefulPreparserListener.settings.toList)
    } else if (queryText.isBlank) {
      throw exceptionFactory.syntaxException(
        GqlHelper.getGql42001_42N45(0, 1, 1),
        EMPTY_QUERY_PARSER_EXCEPTION_MSG,
        InputPosition(0, 1, 1)
      )
    } else {
      (InputPosition(0, 1, 1), List.empty)
    }

    PreParsedStatement(
      queryText.substring(queryStart.offset),
      settings,
      queryStart
    )

  }

  private def preParserOptionsNotifications(preParsedStatement: PreParsedStatement): List[InternalNotification] =
    preParsedStatement.options.flatMap { option =>
      option.key.toLowerCase(Locale.ROOT) match {
        case CypherConnectComponentsPlannerOption.key =>
          Some(DeprecatedConnectComponentsPlannerPreParserOption(option.position))
        case CypherEagerAnalyzerOption.key =>
          Some(DeprecatedEagerAnalyzerPreParserOption(option.position))
        case CypherPlannerVersionOption.key if CypherPlannerVersionOption.isRetired(option.value) =>
          Some(RetiredPlannerVersionPreParserOption(option.position, option.value))
        case _ =>
          None
      }
    }

  // Used as an optimisation to shortcut preparsing.
  private def hasPreparserOptions(token: Int): Boolean = {
    token == CypherPreparserParser.CYPHER ||
    token == CypherPreparserParser.EXPLAIN ||
    token == CypherPreparserParser.PROFILE
  }
}

object PreParser {

  /**
   *
   * @param preparsed pre-parsed statement
   * @param configuration dbms configuration
   * @param defaultVersion database specific default query language version
   * @return
   */
  def queryOptions(
    preparsed: PreParsedStatement,
    configuration: CypherConfiguration,
    defaultVersion: CypherVersion
  ): QueryOptions = {
    val queryOptions = CypherQueryOptions.fromValues(configuration, preparsed.options.map(o => (o.key, o.value)).toSet)
    val derivedOptions = CypherQueryOptions.derivedOptions(queryOptions, configuration)
    QueryOptions(
      offset = preparsed.offset,
      queryOptions = queryOptions,
      derivedOptions = derivedOptions,
      defaultLanguage = defaultVersion
    )
  }
}
