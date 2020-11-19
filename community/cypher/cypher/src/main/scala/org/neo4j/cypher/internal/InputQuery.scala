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
import org.neo4j.cypher.internal.options.CypherQueryOptions
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
    s"${options.cacheKey} $statement"

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

  override val cacheKey: String = s"${options.cacheKey} ${state.queryText}"

}

/**
 * Query execution options
 */
case class QueryOptions(offset: InputPosition,
                        isPeriodicCommit: Boolean,
                        queryOptions: CypherQueryOptions,
                        recompilationLimitReached: Boolean = false,
                        materializedEntitiesMode: Boolean = false) {

  def version: CypherVersion = queryOptions.version
  def executionMode: CypherExecutionMode = queryOptions.executionMode
  def planner: CypherPlannerOption = queryOptions.planner
  def runtime: CypherRuntimeOption = queryOptions.runtime
  def updateStrategy: CypherUpdateStrategy = queryOptions.updateStrategy
  def expressionEngine: CypherExpressionEngineOption = queryOptions.expressionEngine
  def operatorEngine: CypherOperatorEngineOption = queryOptions.operatorEngine
  def interpretedPipesFallback: CypherInterpretedPipesFallbackOption = queryOptions.interpretedPipesFallback
  def replan: CypherReplanOption = queryOptions.replan
  def connectComponentsPlanner: CypherConnectComponentsPlannerOption = queryOptions.connectComponentsPlanner
  def debugOptions: CypherDebugOptions = queryOptions.debugOptions

  def compileWhenHot: Boolean = expressionEngine == CypherExpressionEngineOption.onlyWhenHot || expressionEngine == CypherExpressionEngineOption.default

  def useCompiledExpressions: Boolean = expressionEngine == CypherExpressionEngineOption.compiled || (compileWhenHot && recompilationLimitReached)

  def withRecompilationLimitReached: QueryOptions = copy(recompilationLimitReached = true)

  def withExecutionMode(executionMode: CypherExecutionMode): QueryOptions =
    copy(queryOptions = queryOptions.copy(executionMode = executionMode))

  def cacheKey: String = {
    val key = queryOptions.cacheKey
    if (key.isBlank) key else "CYPHER " + key
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
