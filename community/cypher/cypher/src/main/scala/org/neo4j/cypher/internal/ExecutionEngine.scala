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

import java.time.Clock
import java.util.function.Supplier

import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.compatibility.CypherCacheMonitor
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.CompilationTracer.QueryCompilationEvent
import org.neo4j.cypher.{CypherExpressionEngineOption, ParameterNotFoundException, exceptionHandler}
import org.neo4j.graphdb.Result
import org.neo4j.helpers.collection.Pair
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.impl.query.{QueryExecution, ResultBuffer, TransactionalContext}
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.logging.LogProvider
import org.neo4j.values.virtual.MapValue

trait StringCacheMonitor extends CypherCacheMonitor[Pair[String, ParameterTypeMap]]

/**
  * This class constructs and initializes both the cypher compilers and runtimes, which are very expensive
  * operation. Please make sure this will be constructed only once and properly reused.
  */
class ExecutionEngine(val queryService: GraphDatabaseQueryService,
                      val kernelMonitors: Monitors,
                      val tracer: CompilationTracer,
                      val cacheTracer: CacheTracer[Pair[String, ParameterTypeMap]],
                      val config: CypherConfiguration,
                      val compatibilityFactory: CompilerFactory,
                      val logProvider: LogProvider,
                      val clock: Clock = Clock.systemUTC() ) {

  require(queryService != null, "Can't work with a null graph database")

  // HELPER OBJECTS

  private val preParser = new PreParser(config.version, config.planner, config.runtime, config.expressionEngineOption, config.queryCacheSize)
  private val lastCommittedTxIdProvider = LastCommittedTxIdProvider(queryService)
  private def planReusabilitiy(executableQuery: ExecutableQuery,
                               transactionalContext: TransactionalContext): ReusabilityState =
    executableQuery.reusabilityState(lastCommittedTxIdProvider, transactionalContext)

  // Log on stale query discard from query cache
  private val log = logProvider.getLog( getClass )
  kernelMonitors.addMonitorListener( new StringCacheMonitor {
    override def cacheDiscard(ignored: Pair[String, ParameterTypeMap], query: String, secondsSinceReplan: Int) {
      log.info(s"Discarded stale query from the query cache after $secondsSinceReplan seconds: $query")
    }
  })

  private val planStalenessCaller =
    new PlanStalenessCaller[ExecutableQuery](clock,
                                             config.statsDivergenceCalculator,
                                             lastCommittedTxIdProvider,
                                             planReusabilitiy)
  private val queryCache: QueryCache[String,Pair[String, ParameterTypeMap], ExecutableQuery] =
    new QueryCache[String, Pair[String, ParameterTypeMap], ExecutableQuery](config.queryCacheSize, planStalenessCaller, cacheTracer)

  private val masterCompiler: MasterCompiler = new MasterCompiler(config, new CompilerLibrary(compatibilityFactory))

  private val schemaHelper = new SchemaHelper(queryCache)

  // ACTUAL FUNCTIONALITY

  def profile(query: String, params: MapValue, context: TransactionalContext): Result =
    execute(query, params, context, profile = true)

  def execute(query: String, params: MapValue, context: TransactionalContext, profile: Boolean = false): Result = {
    val queryTracer = tracer.compileQuery(query)

    try {
      val preParsedQuery = preParser.preParseQuery(query, profile)
      val executableQuery = getOrCompile(context, preParsedQuery, queryTracer, params)
      if (preParsedQuery.executionMode.name != "explain") {
        checkParameters(executableQuery.paramNames, params, executableQuery.extractedParams)
      }
      val combinedParams = params.updatedWith(executableQuery.extractedParams)
      context.executingQuery().compilationCompleted(executableQuery.compilerInfo, supplier(executableQuery.planDescription()))
      executableQuery.execute(context, preParsedQuery, combinedParams)

    } catch {
      case t: Throwable =>
        context.close(false)
        throw t
    } finally queryTracer.close()
  }

  /*
   * Return the primary and secondary compile to be used
   *
   * The primary compiler is the main compiler and the secondary compiler is used for compiling expressions for hot queries.
   */
  private def compilers(preParsedQuery: PreParsedQuery,
                        tracer: QueryCompilationEvent,
                        transactionalContext: TransactionalContext,
                        params: MapValue): (() => ExecutableQuery, (Int) => Option[ExecutableQuery]) = preParsedQuery.expressionEngine match {
    //check if we need to jit compiling of queries
    case CypherExpressionEngineOption.onlyWhenHot if config.recompilationLimit > 0 =>
      val primary: () => ExecutableQuery = () => masterCompiler.compile(preParsedQuery,
                                                 tracer, transactionalContext, params)
      val secondary: (Int) => Option[ExecutableQuery] =
        count => {
          if (count > config.recompilationLimit) Some(masterCompiler.compile(preParsedQuery.copy(recompilationLimitReached = true),
                                                                             tracer, transactionalContext, params))
          else None
        }

      (primary, secondary)
    //We have recompilationLimit == 0, go to compiled directly
    case CypherExpressionEngineOption.onlyWhenHot =>
      (() => masterCompiler.compile(preParsedQuery.copy(recompilationLimitReached = true),
                                    tracer, transactionalContext, params), (_) => None)
    //In the other cases we have no recompilation step
    case _ =>  (() => masterCompiler.compile(preParsedQuery, tracer, transactionalContext, params), (_) => None)
  }

  private def getOrCompile(context: TransactionalContext,
                           preParsedQuery: PreParsedQuery,
                           tracer: QueryCompilationEvent,
                           params: MapValue
                          ): ExecutableQuery = {
    val cacheKey = Pair.of(preParsedQuery.statementWithVersionAndPlanner, QueryCache.extractParameterTypeMap(params))

    // create transaction and query context
    val tc = context.getOrBeginNewIfClosed()
    val compilerAuthorization = tc.restrictCurrentTransaction(tc.securityContext.withMode(AccessMode.Static.READ))

    try {
      var n = 0
      while (n < ExecutionEngine.PLAN_BUILDING_TRIES) {

        val schemaToken = schemaHelper.readSchemaToken(tc)
        val (primaryCompiler, secondaryCompiler) = compilers(preParsedQuery, tracer, tc, params)
        val cacheLookup = queryCache.computeIfAbsentOrStale(cacheKey,
                                                            tc,
                                                            primaryCompiler,
                                                            secondaryCompiler,
                                                            preParsedQuery.rawStatement)
        cacheLookup match {
          case _: CacheHit[_] |
               _: CacheDisabled[_] =>
            val executableQuery = cacheLookup.executableQuery
            if (schemaHelper.lockLabels(schemaToken, executableQuery, preParsedQuery.version, tc)) {
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

  def clearQueryCaches(): Long =
    List(masterCompiler.clearCaches(), queryCache.clear(), preParser.clearCache()).max

  def isPeriodicCommit(query: String): Boolean =
    preParser.preParseQuery(query, profile = false).isPeriodicCommit

  // HELPERS

  @throws(classOf[ParameterNotFoundException])
  private def checkParameters(queryParams: Seq[String], givenParams: MapValue, extractedParams: MapValue) {
    exceptionHandler.runSafely {
      val missingKeys = queryParams.filter(key => !(givenParams.containsKey(key) || extractedParams.containsKey(key))).distinct
      if (missingKeys.nonEmpty) {
        throw new ParameterNotFoundException("Expected parameter(s): " + missingKeys.mkString(", "))
      }
    }
  }

  private def supplier[T](t: => T): Supplier[T] =
    new Supplier[T] {
      override def get(): T = t
    }
}

object ExecutionEngine {
  val PLAN_BUILDING_TRIES: Int = 20
}
