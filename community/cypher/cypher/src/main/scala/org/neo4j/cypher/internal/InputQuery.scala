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

import org.neo4j.cypher.{internal, _}
import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.prettifier.{ExpressionStringifier, Prettifier}
import org.neo4j.cypher.internal.v4_0.frontend.phases.BaseState
import org.neo4j.cypher.internal.v4_0.util.InputPosition

/**
  * Input to query execution
  */
sealed trait InputQuery {
  def options: QueryOptions

  def description: String

  def cacheKey: AnyRef

  def withRecompilationLimitReached: InputQuery
}

/**
  * Query execution input as a pre-parsed Cypher query.
  */
case class PreParsedQuery(statement: String, rawStatement: String, options: QueryOptions) extends InputQuery {

  val statementWithVersionAndPlanner: String = {
    val f = options.cacheKey
    s"CYPHER ${f.version} ${f.plannerInfo} ${f.runtimeInfo} ${f.updateStrategyInfo} ${f.expressionEngineInfo} ${f.operatorEngineInfo} ${f.debugFlags} $statement"
  }

  override def cacheKey: String = statementWithVersionAndPlanner

  def rawPreparserOptions: String = rawStatement.take(rawStatement.length - statement.length)

  override def description: String = rawStatement

  override def withRecompilationLimitReached: PreParsedQuery = copy(options = options.withRecompilationLimitReached)
}

/**
  * Query execution input as a fully parsed Cypher query.
  */
case class FullyParsedQuery(state: BaseState, options: QueryOptions) extends InputQuery {

  override lazy val description: String = FullyParsedQuery.prettify(this)

  override def withRecompilationLimitReached: FullyParsedQuery = copy(options = options.withRecompilationLimitReached)

  override def cacheKey: FullyParsedQuery.CacheKey = FullyParsedQuery.CacheKey(statement = state.statement(), fields = options.cacheKey)

}

object FullyParsedQuery {

  case class CacheKey(statement: Statement, fields: QueryOptions.CacheKey)

  private val prettifier = Prettifier(ExpressionStringifier())

  private def prettify(query: FullyParsedQuery): String =
    "/* FullyParsedQuery */ " + prettifier.asString(query.state.statement())
}

/**
  * Query execution options
  */
case class QueryOptions(offset: InputPosition,
                        isPeriodicCommit: Boolean,
                        version: CypherVersion,
                        executionMode: CypherExecutionMode,
                        planner: CypherPlannerOption,
                        runtime: CypherRuntimeOption,
                        updateStrategy: CypherUpdateStrategy,
                        expressionEngine: CypherExpressionEngineOption,
                        operatorEngine: CypherOperatorEngineOption,
                        interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
                        debugOptions: Set[String],
                        recompilationLimitReached: Boolean = false,
                        materializedEntitiesMode: Boolean = false) {

  def compileWhenHot: Boolean = expressionEngine == CypherExpressionEngineOption.onlyWhenHot || expressionEngine == CypherExpressionEngineOption.default

  def useCompiledExpressions: Boolean = expressionEngine == CypherExpressionEngineOption.compiled || (compileWhenHot && recompilationLimitReached)

  def withRecompilationLimitReached: QueryOptions = copy(recompilationLimitReached = true)

  def cacheKey: QueryOptions.CacheKey = QueryOptions.CacheKey(
    version = version.name,
    plannerInfo = planner match {
      case CypherPlannerOption.default => ""
      case _ => s"planner=${planner.name}"
    },
    runtimeInfo = runtime match {
      case CypherRuntimeOption.default => ""
      case _ => s"runtime=${runtime.name}"
    },
    updateStrategyInfo = updateStrategy match {
      case CypherUpdateStrategy.default => ""
      case _ => s"updateStrategy=${updateStrategy.name}"
    },
    expressionEngineInfo = expressionEngine match {
      case CypherExpressionEngineOption.default |
           CypherExpressionEngineOption.onlyWhenHot => ""
      case _ => s"expressionEngine=${expressionEngine.name}"
    },
    operatorEngineInfo = operatorEngine match {
      case CypherOperatorEngineOption.default => ""
      case _ => s"operatorEngine=${operatorEngine.name}"
    },
    interpretedPipesFallbackInfo = interpretedPipesFallback match {
      case CypherInterpretedPipesFallbackOption.default => ""
      case _ => s"interpretedPipesFallback=${interpretedPipesFallback.name}"
    },
    debugFlags = debugOptions.map(flag => s"debug=$flag").mkString(" ")
  )

}

object QueryOptions {
  case class CacheKey(version: String,
                      plannerInfo: String,
                      runtimeInfo: String,
                      updateStrategyInfo: String,
                      expressionEngineInfo: String,
                      operatorEngineInfo: String,
                      interpretedPipesFallbackInfo: String,
                      debugFlags: String)

  val default: QueryOptions = QueryOptions(InputPosition.NONE,
    false,
    CypherVersion.default,
    CypherExecutionMode.default,
    CypherPlannerOption.default,
    CypherRuntimeOption.default,
    CypherUpdateStrategy.default,
    CypherExpressionEngineOption.default,
    CypherOperatorEngineOption.default,
    CypherInterpretedPipesFallbackOption.default,
    Set())
}
