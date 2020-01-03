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

import org.neo4j.cypher.internal.compatibility.CypherCurrentCompiler
import org.neo4j.cypher.{CypherPlannerOption, CypherRuntimeOption, CypherUpdateStrategy, CypherVersion}
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap

import scala.collection.JavaConversions._

/**
  * Keeps track of all cypher compilers, and finds the relevant compiler for a preparsed query.
  *
  * @param factory factory to create compilers
  */
class CompilerLibrary(factory: CompilerFactory) {

  private val compilers = new CopyOnWriteHashMap[CompilerKey, Compiler]

  def selectCompiler(cypherVersion: CypherVersion,
                     cypherPlanner: CypherPlannerOption,
                     cypherRuntime: CypherRuntimeOption,
                     cypherUpdateStrategy: CypherUpdateStrategy): Compiler = {
    val key = CompilerKey(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy)
    val compiler = compilers.get(key)
    if (compiler == null) {
      compilers.put(key, factory.createCompiler(cypherVersion, cypherPlanner, cypherRuntime, cypherUpdateStrategy))
      compilers.get(key)
    } else compiler
  }

  def clearCaches(): Long = {
    val numClearedEntries =
      compilers.values().collect {
        case c: CachingPlanner[_] => c.clearCaches()
        case c: CypherCurrentCompiler[_] if c.planner.isInstanceOf[CachingPlanner[_]] =>
          c.planner.asInstanceOf[CachingPlanner[_]].clearCaches()
      }

    if (numClearedEntries.nonEmpty)
      numClearedEntries.max
    else 0
  }

  case class CompilerKey(cypherVersion: CypherVersion,
                         cypherPlanner: CypherPlannerOption,
                         cypherRuntime: CypherRuntimeOption,
                         cypherUpdateStrategy: CypherUpdateStrategy)
}
