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

import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.planning.CypherPlanner

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

/**
 * Keeps track of all cypher compilers, and finds the relevant compiler for a preparsed query.
 *
 * @param factory factory to create compilers
 */
class CompilerLibrary(factory: CompilerFactory, executionEngineProvider: () => ExecutionEngine) {
  def supportsAdministrativeCommands(): Boolean = factory.supportsAdministrativeCommands()

  private val compilers = new ConcurrentHashMap[CompilerKey, Compiler]

  def selectCompiler(cypherVersion: CypherVersion,
                     cypherPlanner: CypherPlannerOption,
                     cypherRuntime: CypherRuntimeOption
                     ): Compiler = {
    val key = CompilerKey(cypherVersion, cypherPlanner, cypherRuntime)
    compilers.computeIfAbsent(key, _ => factory.createCompiler(cypherVersion, cypherPlanner, cypherRuntime,  executionEngineProvider))
  }

  def clearCaches(): Long = {
    val numClearedEntries =
      compilers.values().asScala.collect {
        case c: CypherPlanner => c.clearCaches()
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

  case class CompilerKey(cypherVersion: CypherVersion,
                         cypherPlanner: CypherPlannerOption,
                         cypherRuntime: CypherRuntimeOption
                         )
}
