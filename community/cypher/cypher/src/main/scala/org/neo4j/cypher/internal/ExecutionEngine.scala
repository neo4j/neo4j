/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.time.Clock

import org.neo4j.cypher.internal.compatibility.{CypherCacheMonitor, LFUCache}
import org.neo4j.cypher.internal.frontend.v3_5.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.runtime.interpreted.{LastCommittedTxIdProvider, ValueConversion}
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.CompilationTracer.QueryCompilationEvent
import org.neo4j.cypher.{ParameterNotFoundException, SyntaxException, exceptionHandler}
import org.neo4j.graphdb.Result
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.{QueryExecution, ResultBuffer, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors, Monitors => KernelMonitors}
import org.neo4j.logging.LogProvider
import org.neo4j.values.virtual.{MapValue, VirtualValues}

trait StringCacheMonitor extends CypherCacheMonitor[String]

/**
  * This class constructs and initializes both the cypher compilers and runtimes, which are very expensive
  * operation. Please make sure this will be constructed only once and properly reused.
  */
class ExecutionEngine(val queryService: GraphDatabaseQueryService,
                      val kernelMonitors: Monitors,
                      val tracer: CompilationTracer,
                      val cacheTracer: CacheTracer[String],
                      val config: CypherConfiguration,
                      val compatibilityFactory: CompatibilityFactory,
                      val logProvider: LogProvider,
                      val clock: Clock = Clock.systemUTC() ) {

  require(queryService != null, "Can't work with a null graph database")

  private val preParser = new PreParser(config.version, config.planner, config.runtime, config.queryCacheSize)
  private val lastCommittedTxIdProvider = LastCommittedTxIdProvider(queryService)
  private def planReusabilitiy(cachedExecutableQuery: CachedExecutableQuery,
                               transactionalContext: TransactionalContext): ReusabilityInfo =
    cachedExecutableQuery.plan.reusabilityInfo(lastCommittedTxIdProvider, transactionalContext)

  private val planStalenessCaller =
    new PlanStalenessCaller[CachedExecutableQuery](clock,
                                                   config.statsDivergenceCalculator,
                                                   lastCommittedTxIdProvider,
                                                   planReusabilitiy)
  private val queryCache: NewQueryCache[String, CachedExecutableQuery] =
    new NewQueryCache(config.queryCacheSize, planStalenessCaller, cacheTracer, NewQueryCache.BEING_RECOMPILED)

  private val compilerEngineDelegator: CompilerEngineDelegator =
    new CompilerEngineDelegator(queryService, kernelMonitors, config, logProvider, compatibilityFactory)

  private val parsedQueries = new LFUCache[String, ParsedQuery](config.queryCacheSize)

  private val schemaHelper = new SchemaHelper(queryCache)

  def profile(query: String, params: MapValue, context: TransactionalContext): Result =
    execute(query, params, context, profile = true)

  def execute(query: String, params: MapValue, context: TransactionalContext, profile: Boolean = false): Result = {
    val queryTracer = tracer.compileQuery(query)

    try {
      val preParsedQuery = preParser.preParseQuery(query, profile)
      val cachedExecutableQuery = getOrCompile(context, preParsedQuery, queryTracer)
      if (preParsedQuery.executionMode.name != "explain") {
        checkParameters(cachedExecutableQuery.paramNames, params, cachedExecutableQuery.extractedParams)
      }
      val combinedParams = VirtualValues.combine(params, cachedExecutableQuery.extractedParams)
      cachedExecutableQuery.plan.run(context, preParsedQuery.executionMode, combinedParams)

    } finally queryTracer.close()
  }

  def execute(query: String,
              mapParams: MapValue,
              context: TransactionalContext,
              resultBuffer: ResultBuffer
             ): QueryExecution = {
    ???
  }

  private def getOrCompile(context: TransactionalContext,
                           preParsedQuery: PreParsedQuery,
                           tracer: QueryCompilationEvent
                          ): CachedExecutableQuery = {
    val cacheKey = preParsedQuery.statementWithVersionAndPlanner

    // create transaction and query context
    val tc = context.getOrBeginNewIfClosed()
    val compilerAuthorization = tc.restrictCurrentTransaction(tc.securityContext.withMode(AccessMode.Static.READ))

    try {
      var n = 0
      while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {

        val schemaToken = schemaHelper.readSchemaToken(tc)
        val cacheLookup = queryCache.computeIfAbsentOrStale(cacheKey,
                                                            tc,
                                                            () => compileQuery(preParsedQuery, tracer, tc))
        cacheLookup match {
          case CacheHit(executableQuery) =>
            if (schemaHelper.lockLabels(schemaToken, executableQuery.plan, preParsedQuery.version, tc)) {
              tc.cleanForReuse()
              return executableQuery
            }
          case CacheMiss(executableQuery) =>
            // Do nothing. In the next attempt we will find the plan in the cache and
            // used it unless the schema has changed during planning.
        }

        n += 1
      }
    } finally {
      compilerAuthorization.close()
    }

    throw new IllegalStateException("Could not compile query due to insanely frequent schema changes")
  }

  private def compileQuery(preParsedQuery: PreParsedQuery,
                           tracer: CompilationPhaseTracer,
                           tc: TransactionalContext): CachedExecutableQuery = {

    val parsedQuery = parsePreParsedQuery(preParsedQuery, tracer)
    val (executionPlan, extractedParams, paramNames) = parsedQuery.plan(tc, tracer)
    CachedExecutableQuery(executionPlan, paramNames, ValueConversion.asValues(extractedParams))
  }

  @throws(classOf[SyntaxException])
  private def parsePreParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer): ParsedQuery = {
    parsedQueries.get(preParsedQuery.statementWithVersionAndPlanner).getOrElse {
      val parsedQuery = compilerEngineDelegator.parseQuery(preParsedQuery, tracer)
      //don't cache failed queries
      if (!parsedQuery.hasErrors) parsedQueries.put(preParsedQuery.statementWithVersionAndPlanner, parsedQuery)
      parsedQuery
    }
  }

  def clearQueryCaches(): Long =
    Math.max(parsedQueries.clear(), queryCache.clear())

  def isPeriodicCommit(query: String): Boolean =
    preParser.preParseQuery(query, profile = false).isPeriodicCommit

  // HELPERS

  @throws(classOf[ParameterNotFoundException])
  private def checkParameters(queryParams: Seq[String], givenParams: MapValue, extractedParams: MapValue) {
    exceptionHandler.runSafely {
      val missingKeys = queryParams.filter(key => !(givenParams.containsKey(key) || extractedParams.containsKey(key)))
      if (missingKeys.nonEmpty) {
        throw new ParameterNotFoundException("Expected parameter(s): " + missingKeys.mkString(", "))
      }
    }
  }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
