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

import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.logging.InternalLogProvider
import org.neo4j.monitoring.Monitors

import java.time.Clock

/**
 * This class constructs and initializes both the cypher compilers and runtimes, which are very expensive
 * operation. Please make sure this will be constructed only once and properly reused.
 */
class DefaultExecutionEngine(
  queryService: GraphDatabaseQueryService,
  kernelMonitors: Monitors,
  tracer: CompilationTracer,
  config: CypherConfiguration,
  compilerLibrary: CompilerLibrary,
  queryCaches: CypherQueryCaches,
  logProvider: InternalLogProvider,
  clock: Clock = Clock.systemUTC()
) extends ExecutionEngine(
      queryService,
      kernelMonitors,
      tracer,
      config,
      new LibraryMasterCompiler(compilerLibrary),
      queryCaches,
      logProvider,
      clock
    ) {

  require(queryService != null, "Can't work with a null graph database")

}
