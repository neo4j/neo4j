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

import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.planning.CypherPlanner
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.values.virtual.MapValue

import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Keeps track of all cypher compilers, and finds the relevant compiler for a preparsed query.
 *
 * @param factory factory to create compilers
 */
class CompilerLibrary(factory: CompilerFactory, executionEngineProvider: () => ExecutionEngine) {
  def supportsAdministrativeCommands(): Boolean = factory.supportsAdministrativeCommands()

  protected val compilers = new ConcurrentHashMap[CompilerKey, Compiler]

  def selectCompiler(
    cypherPlanner: CypherPlannerOption,
    cypherRuntime: CypherRuntimeOption,
    materializedEntitiesMode: Boolean
  ): Compiler = {
    val key = CompilerKey(cypherPlanner, cypherRuntime)
    compilers.computeIfAbsent(
      key,
      _ =>
        factory.createCompiler(
          cypherPlanner,
          cypherRuntime,
          materializedEntitiesMode,
          executionEngineProvider
        )
    )
  }

  def clearCaches(): Long = {
    val numClearedEntries =
      compilers.values().asScala.collect {
        case c: CypherPlanner            => c.clearCaches()
        case c: CypherCurrentCompiler[_] => c.clearCaches()
      }

    if (numClearedEntries.nonEmpty)
      numClearedEntries.max
    else 0
  }

  def clearExecutionPlanCaches(): Unit = {
    compilers.values().asScala.collect {
      case c: CypherCurrentCompiler[_] => c.clearExecutionPlanCache()
    }
  }

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = {
    compilers.values().asScala.collect {
      case c: CypherPlanner            => c.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
      case c: CypherCurrentCompiler[_] => c.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
    }
  }

  case class CompilerKey(
    cypherPlanner: CypherPlannerOption,
    cypherRuntime: CypherRuntimeOption
  )
}
