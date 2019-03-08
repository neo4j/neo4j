/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.neo4j.cypher.internal.v3_5.util.InputPosition

/**
  * Representation a pre-parsed Cypher query.
  */
case class PreParsedQuery(statement: String,
                          offset: InputPosition,
                          rawStatement: String,
                          isPeriodicCommit: Boolean,
                          version: CypherVersion,
                          executionMode: CypherExecutionMode,
                          planner: CypherPlannerOption,
                          runtime: CypherRuntimeOption,
                          updateStrategy: CypherUpdateStrategy,
                          expressionEngine: CypherExpressionEngineOption,
                          debugOptions: Set[String],
                          recompilationLimitReached: Boolean = false) {

  val statementWithVersionAndPlanner: String = {
    val plannerInfo = planner match {
      case CypherPlannerOption.default => ""
      case _ => s"planner=${planner.name}"
    }
    val runtimeInfo = runtime match {
      case CypherRuntimeOption.default => ""
      case _ => s"runtime=${runtime.name}"
    }
    val updateStrategyInfo = updateStrategy match {
      case CypherUpdateStrategy.default => ""
      case _ => s"updateStrategy=${updateStrategy.name}"
    }

    val expressionEngineInfo = expressionEngine match {
      case CypherExpressionEngineOption.default | CypherExpressionEngineOption.onlyWhenHot => ""
      case _ => s"expressionEngine=${expressionEngine.name}"
    }

    val debugFlags = debugOptions.map(flag => s"debug=$flag").mkString(" ")

    s"CYPHER ${version.name} $plannerInfo $runtimeInfo $updateStrategyInfo $expressionEngineInfo $debugFlags $statement"
  }

  def rawPreparserOptions: String =
    rawStatement.take(rawStatement.length - statement.length)

  def useCompiledExpressions: Boolean = expressionEngine == CypherExpressionEngineOption.compiled ||
    (expressionEngine == CypherExpressionEngineOption.onlyWhenHot && recompilationLimitReached)
}
