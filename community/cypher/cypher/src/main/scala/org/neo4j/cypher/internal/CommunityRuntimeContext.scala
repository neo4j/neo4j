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

import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.logging.InternalLog

import java.time.Clock

/**
 * The regular community runtime context.
 */
case class CommunityRuntimeContext(
  cypherVersion: CypherVersion,
  tokenContext: ReadTokenContext,
  schemaRead: SchemaRead,
  procedures: Procedures,
  log: InternalLog,
  config: CypherRuntimeConfiguration,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  assertOpen: AssertOpen
) extends RuntimeContext {

  override def compileExpressions: Boolean = false
  override def materializedEntitiesMode: Boolean = false
  override def isCommunity: Boolean = true
}

case class CommunityRuntimeContextManager(log: InternalLog, config: CypherRuntimeConfiguration)
    extends RuntimeContextManager[CommunityRuntimeContext] {

  override def create(
    cypherVersion: CypherVersion,
    tokenContext: ReadTokenContext,
    schemaRead: SchemaRead,
    procedures: Procedures,
    clock: Clock,
    debugOptions: CypherDebugOptions,
    ignore: Boolean,
    ignore2: Boolean,
    ignore3: CypherOperatorEngineOption,
    ignore4: CypherInterpretedPipesFallbackOption,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    assertOpen: AssertOpen
  ): CommunityRuntimeContext =
    CommunityRuntimeContext(
      cypherVersion,
      tokenContext,
      schemaRead,
      procedures,
      log,
      config,
      anonymousVariableNameGenerator,
      assertOpen
    )

  // As we rely completely on transaction bound resources in community,
  // there is no need for further assertions here.
  override def assertAllReleased(): Unit = {}

  override def waitForWorkersToIdle(timeoutMs: Int): Boolean = true
}
