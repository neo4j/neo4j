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

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.neo4j.cypher.CypherReplanOption
import org.neo4j.cypher.internal.QueryCache.NOT_PRESENT
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.compiler.MissingLabelNotification
import org.neo4j.cypher.internal.compiler.MissingPropertyNameNotification
import org.neo4j.cypher.internal.compiler.MissingRelTypeNotification
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * Tracer for cache activity.
 */
trait CacheTracer[QUERY_KEY] {
  /**
   * The item was found in the cache and was not stale.
   */
  def queryCacheHit(queryKey: QUERY_KEY, metaData: String): Unit

  /**
   * The item was not found in the cache or was stale, or a miss was forced by replan=force.
   */
  def queryCacheMiss(queryKey: QUERY_KEY, metaData: String): Unit

  /**
   * The compiler was invoked to compile a key to a query, avoiding JIT compilation.
   */
  def queryCompile(queryKey: QUERY_KEY, metaData: String): Unit

  /**
   * The compiler was invoked to compile a key to a query, requesting JIT compilation.
   */
  def queryJitCompile(queryKey: QUERY_KEY, metaData: String): Unit

  /**
   * The item was found in the cache but has become stale.
   * @param secondsSincePlan how long the last replan was ago
   * @param maybeReason maybe a reason clarifying why the item was stale.
   */
  def queryCacheStale(queryKey: QUERY_KEY, secondsSincePlan: Int, metaData: String, maybeReason: Option[String]): Unit

  /**
   * The query cache was flushed.
   */
  def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit
}

/**
 * A compiler with jit-compilation capabilities.
 */
trait JitCompiler[EXECUTABLE_QUERY] {
  /**
   * Compile a query, avoiding any jit-compilation.
   * If the settings enforce a certain expression engine,
   * this engine is going to be in both compile and jitCompile.
   */
  def compile(): EXECUTABLE_QUERY

  /**
   * Compile a query with jit-compilation.
   * If the settings enforce a certain expression engine,
   * this engine is going to be in both compile and jitCompile.
   */
  def jitCompile(): EXECUTABLE_QUERY

  /**
   * Decide wheter a previously compiled query should be jit-compiled now,
   * and do the jit-compilation in that case.
   * @param hitCount the number of cache hits for that query
   * @return `Some(jit-compiled-query)` if jit-compilation was deemed useful,
   *         `None` otherwise.
   */
  def maybeJitCompile(hitCount: Int): Option[EXECUTABLE_QUERY]
}


sealed trait Staleness
case object NotStale extends Staleness
case class Stale(secondsSincePlan: Int, maybeReason: Option[String]) extends Staleness

/**
 * Callback interface to find out if a query has become stale
 * and should be evicted from the cache.
 */
trait PlanStalenessCaller[EXECUTABLE_QUERY] {
  def staleness(transactionalContext: TransactionalContext,
                cachedExecutableQuery: EXECUTABLE_QUERY): Staleness
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
class QueryCache[QUERY_REP <: AnyRef,
                 QUERY_KEY <: Pair[QUERY_REP, ParameterTypeMap],
                 EXECUTABLE_QUERY <: CacheabilityInfo](
                                                       val maximumSize: Int,
                                                       val stalenessCaller: PlanStalenessCaller[EXECUTABLE_QUERY],
                                                       val tracer: CacheTracer[Pair[QUERY_REP, ParameterTypeMap]]) {

  private val inner: Cache[QUERY_KEY, CachedValue] = Caffeine.newBuilder().maximumSize(maximumSize).build[QUERY_KEY, CachedValue]()

  /*
    * The cached value wraps the value and maintains a count of how many times it has been fetched from the cache
    * and whether or not it has been recompiled.
    */
  private class CachedValue(val value: EXECUTABLE_QUERY, val recompiled: Boolean) {

    @volatile private var _numberOfHits = 0

    def markHit(): Unit = {
      if (!recompiled) {
        _numberOfHits += 1
      }
    }

    def numberOfHits: Int = _numberOfHits

    def canEqual(other: Any): Boolean = other.isInstanceOf[CachedValue]

    override def equals(other: Any): Boolean = other match {
      case that: CachedValue =>
        (that canEqual this) &&
          value == that.value
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(value)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
  }

  /**
   * Retrieve the CachedExecutionPlan associated with the given queryKey, or compile, cache and
   * return the query if it is not in the cache, or the cached execution plan is stale.
   *
   * @param queryKey the queryKey to retrieve the execution plan for
   * @param tc TransactionalContext in which to compile and compute staleness
   * @param compiler Compiler
   * @param metaData String which will be passed to the CacheTracer
   * @return A CacheLookup with an CachedExecutionPlan
   */
  def computeIfAbsentOrStale(queryKey: QUERY_KEY,
                             tc: TransactionalContext,
                             compiler: JitCompiler[EXECUTABLE_QUERY],
                             replanStrategy: CypherReplanOption,
                             metaData: String = ""
                            ): EXECUTABLE_QUERY = {
    if (maximumSize == 0) {
      val result = compiler.compile()
      tracer.queryCompile(queryKey, metaData)
      result
    } else {
      inner.getIfPresent(queryKey) match {
        case NOT_PRESENT =>
          if (replanStrategy == CypherReplanOption.force)
            jitCompileAndCache(queryKey, compiler, metaData)
          else
            compileAndCache(queryKey, compiler, metaData)

        case cachedValue =>
          //mark as seen from cache
          cachedValue.markHit()

          (stalenessCaller.staleness(tc, cachedValue.value), replanStrategy) match {
            case (_, CypherReplanOption.force) =>
              jitCompileAndCache(queryKey, compiler, metaData)
            case (_, CypherReplanOption.skip) =>
              hit(queryKey, cachedValue, metaData)
            case (NotStale, _) =>
              if(invalidNotificationExisting(cachedValue, tc)) {
                compileAndCache(queryKey, compiler, metaData, hitCache = true)
              } else {
                recompileOrGet(cachedValue, compiler, queryKey, metaData)
              }
            case (Stale(secondsSincePlan, maybeReason), _) =>
              tracer.queryCacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
              if (cachedValue.recompiled) jitCompileAndCache(queryKey, compiler, metaData)
              else compileAndCache(queryKey, compiler, metaData)
          }
      }
    }
  }

  /**
   * Check if certain warnings are not valid anymore.
   */
  def invalidNotificationExisting(cachedValue: CachedValue, tc: TransactionalContext): Boolean = {
    cachedValue.value.notifications.exists {
      case notification: MissingLabelNotification =>
        tc.kernelTransaction().tokenRead().nodeLabel(notification.label) != TokenRead.NO_TOKEN
      case notification: MissingRelTypeNotification =>
        tc.kernelTransaction().tokenRead().relationshipType(notification.relType) != TokenRead.NO_TOKEN
      case notification: MissingPropertyNameNotification =>
        tc.kernelTransaction().tokenRead().propertyKey(notification.name) != TokenRead.NO_TOKEN
      case _ => false
    }
  }

  /**
   * JIT recompile a query if needed. Otherwise return the cached value.
   */
  def recompileOrGet(cachedValue: CachedValue,
                     compiler: JitCompiler[EXECUTABLE_QUERY],
                     queryKey: QUERY_KEY,
                     metaData: String
                    ): EXECUTABLE_QUERY = {
    tracer.queryCacheHit(queryKey, metaData)
    val newCachedValue = if (!cachedValue.recompiled ) {
      compiler.maybeJitCompile(cachedValue.numberOfHits) match {
        case Some(recompiledQuery) =>
          tracer.queryJitCompile(queryKey, metaData)
          val recompiled = new CachedValue(recompiledQuery, recompiled = true)
          inner.put(queryKey, recompiled)
          recompiled
        case None => cachedValue
      }
    } else cachedValue

    newCachedValue.value
  }

  private def compileAndCache(queryKey: QUERY_KEY,
                              compiler: JitCompiler[EXECUTABLE_QUERY],
                              metaData: String,
                              hitCache: Boolean = false
                             ): EXECUTABLE_QUERY =
    compileOrJitCompileAndCache(queryKey, () => compiler.compile(), () => tracer.queryCompile(queryKey, metaData), metaData, hitCache)

  private def jitCompileAndCache(queryKey: QUERY_KEY,
                                 compiler: JitCompiler[EXECUTABLE_QUERY],
                                 metaData: String,
                                 hitCache: Boolean = false
                                ): EXECUTABLE_QUERY =
    compileOrJitCompileAndCache(queryKey, () => compiler.jitCompile(), () => tracer.queryJitCompile(queryKey, metaData), metaData, hitCache)

  /**
   * Ensure this query is recompiled and put it in the cache.
   *
   * Compilation is either done in this thread, or by some other thread if it got there
   * first. Regardless of who does it, this is treated as a cache miss, because it will
   * take a long time. The only exception is if hitCache is true, which should only happen
   * when we are forced to recompile due to previously present warnings not being valid anymore
   */
  private def compileOrJitCompileAndCache(queryKey: QUERY_KEY,
                                          compile: () => EXECUTABLE_QUERY,
                                          trace: () => Unit,
                                          metaData: String,
                                          hitCache: Boolean = false
                                         ): EXECUTABLE_QUERY = {
    val newExecutableQuery = compile()
    val result = if (newExecutableQuery.shouldBeCached) {
      val cachedValue = new CachedValue(newExecutableQuery, recompiled = false)
      inner.put(queryKey, cachedValue)
      if (hitCache)
        hit(queryKey, cachedValue, metaData)
      else
        miss(queryKey, newExecutableQuery, metaData)
    } else {
      miss(queryKey, newExecutableQuery, metaData)
    }
    trace()
    result
  }

  private def hit(queryKey: QUERY_KEY,
                  executableQuery: CachedValue,
                  metaData: String
                 ): EXECUTABLE_QUERY = {
    tracer.queryCacheHit(queryKey, metaData)
    executableQuery.value
  }

  private def miss(queryKey: QUERY_KEY,
                   newExecutableQuery: EXECUTABLE_QUERY,
                   metaData: String
                  ): EXECUTABLE_QUERY = {
    tracer.queryCacheMiss(queryKey, metaData)
    newExecutableQuery
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

object QueryCache {
  val NOT_PRESENT: ExecutableQuery = null
  type ParameterTypeMap = Map[String, Class[_]]

  /**
   * Use this method to extract ParameterTypeMap from MapValue that represents parameters
   */
  def extractParameterTypeMap(value: MapValue): ParameterTypeMap = {
    val resultMap = Map.newBuilder[String, Class[_]]
    for(key <- value.keySet().iterator().asScala) {
      resultMap += ((key, value.get(key).getClass))
    }
    resultMap.result()
  }
}
