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

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.{MapValue, VirtualValues}

/**
  * The result of one cache lookup.
  */
sealed trait CacheLookup[EXECUTABLE_QUERY]
case class CacheHit[EXECUTABLE_QUERY](executableQuery: EXECUTABLE_QUERY) extends CacheLookup[EXECUTABLE_QUERY]
case class CacheMiss[EXECUTABLE_QUERY](executableQuery: EXECUTABLE_QUERY) extends CacheLookup[EXECUTABLE_QUERY]

/**
  * Tracer for cache activity.
  */
trait CacheTracer[QUERY_KEY] {
  def queryCacheHit(queryKey: QUERY_KEY): Unit

  def queryCacheMiss(queryKey: QUERY_KEY): Unit

  def queryCacheStale(queryKey: QUERY_KEY, secondsSincePlan: Int): Unit

  def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit
}

/**
  * Cache which maps query strings into CachedExecutableQueries.
  *
  * This cache knows that CachedExecutableQueries can become stale, and uses a
  * PlanStalenessCaller to verify that CEQs are reusable before returning. A CEQ
  * which is detected in the cache, but is found to be stale
  *
  * @param maximumSize Maximum size of this cache
  * @param stalenessCaller Decided whether CachedExecutionPlans are stale
  * @param tracer Traces cache activity
  */
class NewQueryCache[QUERY_KEY <: AnyRef, EXECUTABLE_QUERY <: AnyRef](val maximumSize: Int,
                                                                     val stalenessCaller: PlanStalenessCaller[EXECUTABLE_QUERY],
                                                                     val tracer: CacheTracer[QUERY_KEY],
                                                                     val BEING_RECOMPILED: EXECUTABLE_QUERY) {

  val inner: Cache[QUERY_KEY, EXECUTABLE_QUERY] = Caffeine.newBuilder().maximumSize(maximumSize).build[QUERY_KEY, EXECUTABLE_QUERY]()

  import NewQueryCache.NOT_PRESENT

  /**
    * Retrieve the CachedExecutionPlan associated with the given queryKey, or compile, cache and
    * return the query if it is not in the cache, or the cached execution plan is stale.
    *
    * @param queryKey the queryKey to retrieve the execution plan for
    * @param tc TransactionalContext in which to compile and compute staleness
    * @param compiler Compiler to use if the query is not cached or stale
    * @return A CacheLookup with an CachedExecutionPlan
    */
  def computeIfAbsentOrStale(queryKey: QUERY_KEY,
                             tc: TransactionalContext,
                             compiler: TransactionalContext => EXECUTABLE_QUERY
                            ): CacheLookup[EXECUTABLE_QUERY] = {
    if (maximumSize == 0)
      CacheMiss(compiler(tc))
    else {
      inner.getIfPresent(queryKey) match {
        case NOT_PRESENT =>
          compileAndCache(queryKey, tc, compiler)

        case BEING_RECOMPILED =>
          awaitConcurrentReplan(queryKey)

        case executableQuery =>
          stalenessCaller.staleness(tc, executableQuery) match {
            case NotStale =>
              hit(queryKey, executableQuery)
            case Stale(secondsSincePlan) =>
              tracer.queryCacheStale(queryKey, secondsSincePlan)
              compileAndCache(queryKey, tc, compiler)
          }
      }
    }
  }

  /**
    * Ensure this query is recompiled and put it in the cache.
    *
    * Compilation is either done in this thread, or by some other thread if it got there
    * first. Regardless of who does it, this is treated as a cache miss, because it will
    * take a long time.
    */
  private def compileAndCache(queryKey: QUERY_KEY,
                        tc: TransactionalContext,
                        compiler: TransactionalContext => EXECUTABLE_QUERY
                       ): CacheLookup[EXECUTABLE_QUERY] = {
    val currentValue = inner.asMap().put(queryKey, BEING_RECOMPILED)
    currentValue match {
      case BEING_RECOMPILED =>
        awaitConcurrentReplan(queryKey)

      case x =>
        val newExecutableQuery = compiler(tc)
        inner.put(queryKey, newExecutableQuery)
        miss(queryKey, newExecutableQuery)
    }
  }

  /**
    * Some other thread already started compiling this query. Let's what for that and use that plan.
    */
  private def awaitConcurrentReplan(queryKey: QUERY_KEY): CacheMiss[EXECUTABLE_QUERY] = {
    var cachedExecutableQuery = inner.getIfPresent(queryKey)
    while (cachedExecutableQuery == BEING_RECOMPILED) {
      Thread.`yield`()
      cachedExecutableQuery = inner.getIfPresent(queryKey)
    }
    miss(queryKey, cachedExecutableQuery)
  }

  private def hit(queryKey: QUERY_KEY, executableQuery: EXECUTABLE_QUERY) = {
    tracer.queryCacheHit(queryKey)
    CacheHit(executableQuery)
  }

  private def miss(queryKey: QUERY_KEY, newExecutableQuery: EXECUTABLE_QUERY) = {
    tracer.queryCacheMiss(queryKey)
    CacheMiss(newExecutableQuery)
  }

  /**
    * Method for clearing the LRUCache
    *
    * @return the number of elements in the cache prior to the clearing
    */
  def clear(): Long = {
    val priorSize = inner.estimatedSize()
    inner.invalidateAll()
    inner.cleanUp()
    tracer.queryCacheFlush(priorSize)
    priorSize
  }
}

object NewQueryCache {
  val BEING_RECOMPILED_PLAN: ExecutionPlan = new ExecutionPlan {
    override def reusabilityInfo(lastCommittedTxId: () => Long, ctx: TransactionalContextWrapper): ReusabilityInfo = ???
    override def run(transactionalContext: TransactionalContext, executionMode: CypherExecutionMode, params: MapValue): Result = ???
    override val plannerInfo: PlannerInfo = null
  }
  val BEING_RECOMPILED = CachedExecutableQuery(BEING_RECOMPILED_PLAN, Nil, VirtualValues.emptyMap())
  val NOT_PRESENT: CachedExecutableQuery = null
}
