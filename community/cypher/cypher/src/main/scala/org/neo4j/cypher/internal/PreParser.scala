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

import org.neo4j.cypher._
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.exceptions.{InvalidArgumentException, SyntaxException}
import org.neo4j.cypher.internal.PreParser._

import scala.util.matching.Regex

/**
  * Preparses Cypher queries.
  *
  * The PreParser converts queries like
  *
  *   'CYPHER 3.5 planner=cost,runtime=slotted MATCH (n) RETURN n'
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
class PreParser(configuredVersion: CypherVersion,
                configuredPlanner: CypherPlannerOption,
                configuredRuntime: CypherRuntimeOption,
                configuredExpressionEngine: CypherExpressionEngineOption,
                configuredOperatorEngine: CypherOperatorEngineOption,
                configuredInterpretedPipesFallback: CypherInterpretedPipesFallbackOption,
                planCacheSize: Int) {

  private val preParsedQueries = new LFUCache[String, PreParsedQuery](planCacheSize)

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
    * @param queryText the query
    * @param profile true if the query should be profiled even if profile is not given as a pre-parser option
    * @throws SyntaxException if there are syntactic errors in the pre-parser options
    * @return the pre-parsed query
    */
  @throws(classOf[SyntaxException])
  def preParseQuery(queryText: String, profile: Boolean = false): PreParsedQuery = {
    val preParsedQuery = preParsedQueries.computeIfAbsent(queryText, actuallyPreParse(queryText))
    if (profile) preParsedQuery.copy(options = preParsedQuery.options.copy(executionMode = CypherExecutionMode.profile))
    else preParsedQuery
  }

  private def actuallyPreParse(queryText: String): PreParsedQuery = {
    val preParsedStatement = CypherPreParser(queryText)
    val isPeriodicCommit = PreParser.periodicCommitHintRegex.findFirstIn(preParsedStatement.statement.toUpperCase).nonEmpty

    val options = queryOptions(preParsedStatement.options,
      preParsedStatement.offset,
      isPeriodicCommit,
      configuredVersion,
      configuredPlanner,
      configuredRuntime,
      configuredExpressionEngine,
      configuredOperatorEngine,
      configuredInterpretedPipesFallback)

    PreParsedQuery(preParsedStatement.statement, queryText, options)
  }


}

object PreParser {
  val periodicCommitHintRegex: Regex = "^\\s*USING\\s+PERIODIC\\s+COMMIT.*".r

  private final val ILLEGAL_PLANNER_RUNTIME_COMBINATIONS: Set[(CypherPlannerOption, CypherRuntimeOption)] = Set.empty
  private final val ILLEGAL_PLANNER_VERSION_COMBINATIONS: Set[(CypherPlannerOption, CypherVersion)] = Set.empty
  private final val ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherExpressionEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.compiled),
      (CypherExpressionEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final val ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS: Set[(CypherOperatorEngineOption, CypherRuntimeOption)] =
    Set(
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.compiled),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.slotted),
      (CypherOperatorEngineOption.compiled, CypherRuntimeOption.interpreted))
  private final val ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS: Set[(CypherInterpretedPipesFallbackOption, CypherRuntimeOption)] =
    Set(
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.compiled),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.disabled, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.compiled),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.whitelistedPlansOnly, CypherRuntimeOption.interpreted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.compiled),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.slotted),
      (CypherInterpretedPipesFallbackOption.allPossiblePlans, CypherRuntimeOption.interpreted)
    )


  private class PPOption[T](val default: T) {
    var selected: Option[T] = None

    def pick: T = selected.getOrElse(default)
    def isSelected: Boolean = selected.nonEmpty
    def selectOrThrow(t: T, errorMsg: String): Unit =
      selected match {
        case Some(previous) => if (previous != t) throw new InvalidPreparserOption(errorMsg)
        case None => selected = Some(t)
      }
  }

  class InvalidPreparserOption(msg: String) extends InvalidArgumentException(msg)

  def queryOptions(options: Seq[PreParserOption],
                   offset: InputPosition,
                   isPeriodicCommit: Boolean,
                   configuredVersion: CypherVersion,
                   configuredPlanner: CypherPlannerOption,
                   configuredRuntime: CypherRuntimeOption,
                   configuredExpressionEngine: CypherExpressionEngineOption,
                   configuredOperatorEngine: CypherOperatorEngineOption,
                   configuredInterpretedPipesFallback: CypherInterpretedPipesFallbackOption): QueryOptions = {
    val executionMode: PPOption[CypherExecutionMode] = new PPOption(CypherExecutionMode.default)
    val version: PPOption[CypherVersion] = new PPOption(configuredVersion)
    val planner: PPOption[CypherPlannerOption] = new PPOption(configuredPlanner)
    val runtime: PPOption[CypherRuntimeOption] = new PPOption(configuredRuntime)
    val expressionEngine: PPOption[CypherExpressionEngineOption] = new PPOption(configuredExpressionEngine)
    val operatorEngine: PPOption[CypherOperatorEngineOption] = new PPOption(configuredOperatorEngine)
    val interpretedPipesFallback: PPOption[CypherInterpretedPipesFallbackOption] = new PPOption(configuredInterpretedPipesFallback)
    val updateStrategy: PPOption[CypherUpdateStrategy] = new PPOption(CypherUpdateStrategy.default)
    var debugOptions: Set[String] = Set()

    def parseOptions(options: Seq[PreParserOption]): Unit =
      for (option <- options) {
        option match {
          case e: ExecutionModePreParserOption =>
            executionMode.selectOrThrow(CypherExecutionMode(e.name), "Can't specify multiple conflicting Cypher execution modes")
          case VersionOption(v) =>
            version.selectOrThrow(CypherVersion(v), "Can't specify multiple conflicting Cypher versions")
          case p: PlannerPreParserOption if p.name == GreedyPlannerOption.name =>
            throw new InvalidArgumentException("The greedy planner has been removed in Neo4j 3.1. Please use the cost planner instead.")
          case p: PlannerPreParserOption =>
            planner.selectOrThrow(CypherPlannerOption(p.name), "Can't specify multiple conflicting Cypher planners")
          case r: RuntimePreParserOption =>
            runtime.selectOrThrow(CypherRuntimeOption(r.name), "Can't specify multiple conflicting Cypher runtimes")
          case u: UpdateStrategyOption =>
            updateStrategy.selectOrThrow( CypherUpdateStrategy(u.name), "Can't specify multiple conflicting update strategies")
          case DebugOption(debug) =>
            debugOptions = debugOptions + debug.toLowerCase()
          case engine: ExpressionEnginePreParserOption =>
            expressionEngine.selectOrThrow(CypherExpressionEngineOption(engine.name), "Can't specify multiple conflicting expression engines")
          case o: OperatorEnginePreParserOption =>
            operatorEngine.selectOrThrow(CypherOperatorEngineOption(o.name), "Can't specify multiple conflicting operator execution modes")
          case i: InterpretedPipesFallbackPreParserOption =>
            interpretedPipesFallback.selectOrThrow(CypherInterpretedPipesFallbackOption(i.name), "Can't specify multiple conflicting interpreted pipes fallback modes")

          case ConfigurationOptions(versionOpt, innerOptions) =>
            for (v <- versionOpt)
              version.selectOrThrow(CypherVersion(v.version), "Can't specify multiple conflicting Cypher versions")
            parseOptions(innerOptions)
        }
      }

    parseOptions(options)

    if (ILLEGAL_PLANNER_RUNTIME_COMBINATIONS((planner.pick, runtime.pick)))
      throw new InvalidPreparserOption(s"Unsupported PLANNER - RUNTIME combination: ${planner.pick.name} - ${runtime.pick.name}")

    // Only disallow using rule if incompatible version is explicitly requested
    if (version.isSelected && ILLEGAL_PLANNER_VERSION_COMBINATIONS((planner.pick, version.pick)))
      throw new InvalidArgumentException(s"Unsupported PLANNER - VERSION combination: ${planner.pick.name} - ${version.pick.name}")

    if (runtime.isSelected && expressionEngine.isSelected && ILLEGAL_EXPRESSION_ENGINE_RUNTIME_COMBINATIONS((expressionEngine.pick, runtime.pick)))
      throw new InvalidPreparserOption(s"Cannot combine EXPRESSION ENGINE '${expressionEngine.pick.name}' with RUNTIME '${runtime.pick.name}'")

    if (runtime.isSelected && operatorEngine.isSelected && ILLEGAL_OPERATOR_ENGINE_RUNTIME_COMBINATIONS(operatorEngine.pick, runtime.pick)) {
      throw new InvalidPreparserOption(s"Cannot combine OPERATOR ENGINE '${operatorEngine.pick.name}' with RUNTIME '${runtime.pick.name}'")
    }

    if (runtime.isSelected && interpretedPipesFallback.isSelected && ILLEGAL_INTERPRETED_PIPES_FALLBACK_RUNTIME_COMBINATIONS(interpretedPipesFallback.pick, runtime.pick)) {
      throw new InvalidPreparserOption(s"Cannot combine INTERPRETED PIPES FALLBACK '${interpretedPipesFallback.pick.name}' with RUNTIME '${runtime.pick.name}'")
    }

    QueryOptions(offset,
      isPeriodicCommit,
      version.pick,
      executionMode.pick,
      planner.pick,
      runtime.pick,
      updateStrategy.pick,
      expressionEngine.pick,
      operatorEngine.pick,
      interpretedPipesFallback.pick,
      debugOptions)
  }
}
