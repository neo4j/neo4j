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
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

import java.time.Clock

object MasterCompiler {
  val CLOCK: Clock = Clock.systemUTC()
}

trait MasterCompiler {

  /**
   * Clear all compiler caches.
   *
   * @return the maximum number of entries clear from any cache
   */
  def clearCaches(): Long

  def clearExecutionPlanCaches(): Unit

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit

  /**
   * Compile submitted query into executable query.
   *
   * @param query                query to convert
   * @param tracer               compilation tracer to which events of the compilation process are reported
   * @param transactionalContext transactional context to use during compilation (in logical and physical planning)
   * @return a compiled and executable query
   */
  def compile(
    query: InputQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    notificationLogger: InternalNotificationLogger
  ): ExecutableQuery

  def supportsAdministrativeCommands(): Boolean
}

/**
 * MasterCompiler that uses a single compiler
 */
class SingleMasterCompiler(compiler: Compiler) extends MasterCompiler {

  def clearCaches(): Long = compiler match {
    case c: CypherCurrentCompiler[_] => c.clearCaches()
    case _                           => 0
  }

  def clearExecutionPlanCaches(): Unit = compiler match {
    case c: CypherCurrentCompiler[_] => c.clearExecutionPlanCache()
    case _                           => ()
  }

  def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = compiler match {
    case c: CypherCurrentCompiler[_] => c.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
    case _                           => ()
  }

  override def compile(
    query: InputQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    notificationLogger: InternalNotificationLogger
  ): ExecutableQuery =
    compiler.compile(query, tracer, transactionalContext, params, notificationLogger)

  def supportsAdministrativeCommands(): Boolean = false
}

/**
 * Selects the correct cypher implementation based on a pre-parsed query.
 */
class LibraryMasterCompiler(compilerLibrary: CompilerLibrary) extends MasterCompiler {

  def clearCaches(): Long = {
    compilerLibrary.clearCaches()
  }

  def clearExecutionPlanCaches(): Unit = {
    compilerLibrary.clearExecutionPlanCaches()
  }

  override def insertIntoCache(
    preParsedQuery: PreParsedQuery,
    params: MapValue,
    parsedQuery: BaseState,
    parsingNotifications: Set[InternalNotification]
  ): Unit = {
    compilerLibrary.insertIntoCache(preParsedQuery, params, parsedQuery, parsingNotifications)
  }

  def compile(
    query: InputQuery,
    tracer: CompilationPhaseTracer,
    transactionalContext: TransactionalContext,
    params: MapValue,
    notificationLogger: InternalNotificationLogger
  ): ExecutableQuery = {

    // Do the compilation
    val compiler = compilerLibrary.selectCompiler(
      query.options.queryOptions.planner,
      query.options.queryOptions.runtime
    )

    compiler.compile(query, tracer, transactionalContext, params, notificationLogger)
  }

  def supportsAdministrativeCommands(): Boolean = compilerLibrary.supportsAdministrativeCommands()
}
