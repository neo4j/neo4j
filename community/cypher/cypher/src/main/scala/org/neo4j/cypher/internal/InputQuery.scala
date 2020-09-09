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

import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.options.CypherConnectComponentsPlannerOption
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherUpdateStrategy
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Input to query execution
 */
sealed trait InputQuery {
  def options: QueryOptions

  def description: String

  def cacheKey: String

  def withRecompilationLimitReached: InputQuery
}

/**
 * Query execution input as a pre-parsed Cypher query.
 */
case class PreParsedQuery(statement: String, rawStatement: String, options: QueryOptions) extends InputQuery {

  val statementWithVersionAndPlanner: String =
    s"${options.cacheKey.render} $statement"

  override def cacheKey: String = statementWithVersionAndPlanner

  def rawPreparserOptions: String = rawStatement.take(rawStatement.length - statement.length)

  override def description: String = rawStatement

  override def withRecompilationLimitReached: PreParsedQuery = copy(options = options.withRecompilationLimitReached)
}

/**
 * Query execution input as a fully parsed Cypher query.
 */
case class FullyParsedQuery(state: BaseState, options: QueryOptions) extends InputQuery {

  override lazy val description: String = state.queryText

  override def withRecompilationLimitReached: FullyParsedQuery = copy(options = options.withRecompilationLimitReached)

  override val cacheKey: String = s"${options.cacheKey.render} ${state.queryText}"

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
                        replan: CypherReplanOption,
                        connectComponentsPlanner: CypherConnectComponentsPlannerOption,
                        debugOptions: CypherDebugOptions,
                        recompilationLimitReached: Boolean = false,
                        materializedEntitiesMode: Boolean = false) {

  def compileWhenHot: Boolean = expressionEngine == CypherExpressionEngineOption.onlyWhenHot || expressionEngine == CypherExpressionEngineOption.default

  def useCompiledExpressions: Boolean = expressionEngine == CypherExpressionEngineOption.compiled || (compileWhenHot && recompilationLimitReached)

  def withRecompilationLimitReached: QueryOptions = copy(recompilationLimitReached = true)

  def cacheKey: QueryOptions.CacheKey = QueryOptions.CacheKey(
    version = version.render,
    executionMode match {
      case CypherExecutionMode.profile => "PROFILE"
      case _ => ""
    },
    plannerInfo = planner.render,
    runtimeInfo = runtime.render,
    updateStrategyInfo = updateStrategy.render,
    expressionEngineInfo = expressionEngine.render,
    operatorEngineInfo = operatorEngine.render,
    interpretedPipesFallbackInfo = interpretedPipesFallback.render,
    connectComponentsPlannerInfo = connectComponentsPlanner.render,
    debugFlags = debugOptions.enabledOptionsSeq.map(_.render).mkString(" ")
  )

  def render: Option[String] = {
    val parts = Seq(
      Seq(
        version.render,
        planner.render,
        runtime.render,
        updateStrategy.render,
        expressionEngine.render,
        operatorEngine.render,
        interpretedPipesFallback.render,
        replan.render,
        connectComponentsPlanner.render,
      ),
      debugOptions.enabledOptionsSeq.map(_.render)
    ).flatten.filterNot(_.isEmpty)

    if (parts.nonEmpty) Some(s"CYPHER ${parts.mkString(" ")}")
    else None
  }

}

object QueryOptions {
  case class CacheKey(version: String,
                      profile: String,
                      plannerInfo: String,
                      runtimeInfo: String,
                      updateStrategyInfo: String,
                      expressionEngineInfo: String,
                      operatorEngineInfo: String,
                      interpretedPipesFallbackInfo: String,
                      connectComponentsPlannerInfo: String,
                      debugFlags: String) {
    def render: String =
      s"CYPHER $version $profile $plannerInfo $runtimeInfo $updateStrategyInfo $expressionEngineInfo $operatorEngineInfo $interpretedPipesFallbackInfo $connectComponentsPlannerInfo $debugFlags"
  }

  val default: QueryOptions = QueryOptions(
    InputPosition.NONE,
    isPeriodicCommit = false,
    CypherVersion.default,
    CypherExecutionMode.default,
    CypherPlannerOption.default,
    CypherRuntimeOption.default,
    CypherUpdateStrategy.default,
    CypherExpressionEngineOption.default,
    CypherOperatorEngineOption.default,
    CypherInterpretedPipesFallbackOption.default,
    CypherReplanOption.default,
    CypherConnectComponentsPlannerOption.default,
    CypherDebugOptions.default,
  )
}
