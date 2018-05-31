/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_1

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_1.{CypherCompilerConfiguration, CypherCompilerFactory}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

case class Rule31Compiler(graph: GraphDatabaseQueryService,
                          config: CypherCompilerConfiguration,
                          clock: Clock,
                          kernelMonitors: KernelMonitors) extends Cypher31Compiler {
  protected val compiler = {
    val monitors = WrappedMonitors(kernelMonitors)
    CypherCompilerFactory.ruleBasedCompiler(graph, config, clock, monitors, rewriterSequencer, typeConversions)
  }

  override val queryCacheSize: Int = config.queryCacheSize
}
