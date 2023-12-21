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

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.RemovalCause
import com.github.benmanes.caffeine.cache.RemovalListener
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.QueryCache.NOT_PRESENT
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.notifications.MissingLabelNotification
import org.neo4j.notifications.MissingPropertyNameNotification
import org.neo4j.notifications.MissingRelTypeNotification
import org.neo4j.values.virtual.MapValue

import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * For tracing when the key is CacheKey[T]
 */
trait QueryCacheTracer[InnerKey] extends CacheTracer[CacheKey[InnerKey]]

/**
 * A compiler with expression code generation capabilities.
 */
trait CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY] {

  /**
   * Compile a query, avoiding any expression code generation.
   * If the settings enforce a certain expression engine,
   * this engine is going to be in both compile and compileWithExpressionCodeGen.
   */
  def compile(): EXECUTABLE_QUERY

  /**
   * Compile a query with expression code generation.
   * If the settings enforce a certain expression engine,
   * this engine is going to be in both compile and compileWithExpressionCodeGen.
   */
  def compileWithExpressionCodeGen(): EXECUTABLE_QUERY

  /**
   * Decide wheter a previously compiled query should be using expression code generation now,
   * and do that in that case.
   * @param hitCount the number of cache hits for that query
   * @return `Some(compiled-query-with-expression-code-gen)` if expression code generation was deemed useful,
   *         `None` otherwise.
   */
  def maybeCompileWithExpressionCodeGen(hitCount: Int): Option[EXECUTABLE_QUERY]
}

sealed trait Staleness
case object NotStale extends Staleness
case class Stale(secondsSincePlan: Int, maybeReason: Option[String]) extends Staleness

/**
 * Callback interface to find out if a query has become stale
 * and should be evicted from the cache.
 */
trait PlanStalenessCaller[EXECUTABLE_QUERY] {
  def staleness(transactionalContext: TransactionalContext, cachedExecutableQuery: EXECUTABLE_QUERY): Staleness
}

trait ExecutingQueryTracer {

  /**
   * The item was found in the cache and was not stale.
   */
  def cacheHit(executingQuery: ExecutingQuery): Unit

  /**
   * The item was not found in the cache.
   */
  def cacheMiss(executingQuery: ExecutingQuery): Unit
}

object ExecutingQueryTracer {

  object NoOp extends ExecutingQueryTracer {
    override def cacheHit(executingQuery: ExecutingQuery): Unit = ()
    override def cacheMiss(executingQuery: ExecutingQuery): Unit = ()
  }
}

/**
 * Cache which maps query strings into CachedExecutableQueries.
 *
 * This cache knows that CachedExecutableQueries can become stale, and uses a
 * PlanStalenessCaller to verify that CEQs are reusable before returning a CEQ
 * which is detected in the cache, but is found to be stale.
 *
 * @param maximumSize Maximum size of this cache
 * @param stalenessCaller Decided whether CachedExecutionPlans are stale
 * @param tracer Traces cache activity
 */
class QueryCache[QUERY_KEY <: AnyRef, EXECUTABLE_QUERY <: CacheabilityInfo](
  val cacheFactory: CaffeineCacheFactory,
  val maximumSize: CacheSize,
  val stalenessCaller: PlanStalenessCaller[EXECUTABLE_QUERY],
  val tracer: CacheTracer[QUERY_KEY],
  val executingQueryTracer: ExecutingQueryTracer
) {

  val removalListener: RemovalListener[QUERY_KEY, CachedValue] =
    (key: QUERY_KEY, value: CachedValue, cause: RemovalCause) => tracer.discard(key, "")

  private val inner: Cache[QUERY_KEY, CachedValue] =
    createInner(cacheFactory, maximumSize, removalListener)

  protected def createInner(
    innerFactory: CaffeineCacheFactory,
    size: CacheSize,
    listener: RemovalListener[QUERY_KEY, CachedValue]
  ): Cache[QUERY_KEY, CachedValue] = {
    innerFactory.createCache[QUERY_KEY, CachedValue](size, listener)
  }

  def estimatedSize(): Long = inner.estimatedSize()

  /*
   * The cached value wraps the value and maintains a count of how many times it has been fetched from the cache
   * and whether or not it has been recompiled with expression code generation.
   */
  class CachedValue(val value: EXECUTABLE_QUERY, val recompiledWithExpressionCodeGen: Boolean) {

    @volatile private var _numberOfHits = 0

    def markHit(): Unit = {
      if (!recompiledWithExpressionCodeGen) {
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
   * @param tc       TransactionalContext in which to compile and compute staleness
   * @param compiler Compiler
   * @param metaData String which will be passed to the CacheTracer
   * @return A CacheLookup with an CachedExecutionPlan
   */
  def computeIfAbsentOrStale(
    queryKey: QUERY_KEY,
    tc: TransactionalContext,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    replanStrategy: CypherReplanOption,
    metaData: String = ""
  ): EXECUTABLE_QUERY = {
    lazy val executingQuery = tc.executingQuery()
    if (maximumSize.currentValue == 0) {
      val result = compiler.compile()
      tracer.compute(queryKey, metaData)
      result
    } else {
      inner.getIfPresent(queryKey) match {
        case NOT_PRESENT =>
          if (replanStrategy == CypherReplanOption.force)
            compileWithExpressionCodeGenAndCache(executingQuery, queryKey, compiler, metaData)
          else
            compileAndCache(executingQuery, queryKey, compiler, metaData)

        case cachedValue =>
          // mark as seen from cache
          cachedValue.markHit()

          replanStrategy match {
            case CypherReplanOption.force =>
              compileWithExpressionCodeGenAndCache(executingQuery, queryKey, compiler, metaData, hitCache = true)
            case CypherReplanOption.skip =>
              hit(executingQuery, queryKey, cachedValue, metaData)
            case CypherReplanOption.default =>
              stalenessCaller.staleness(tc, cachedValue.value) match {
                case NotStale =>
                  if (invalidNotificationExisting(cachedValue, tc)) {
                    compileAndCache(executingQuery, queryKey, compiler, metaData, hitCache = true)
                  } else {
                    recompileOrGet(executingQuery, cachedValue, compiler, queryKey, metaData)
                  }
                case Stale(secondsSincePlan, maybeReason) =>
                  tracer.cacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
                  if (cachedValue.recompiledWithExpressionCodeGen)
                    compileWithExpressionCodeGenAndCache(executingQuery, queryKey, compiler, metaData, hitCache = true)
                  else compileAndCache(executingQuery, queryKey, compiler, metaData, hitCache = true)
              }
          }
      }
    }
  }

  /**
   * Check if certain warnings are not valid anymore.
   */
  private def invalidNotificationExisting(cachedValue: CachedValue, tc: TransactionalContext): Boolean = {
    val notifications = cachedValue.value.notifications
    var i = 0
    while (i < notifications.length) {
      if (isInvalidNotification(notifications(i), tc)) {
        return true
      }
      i += 1
    }
    false
  }

  private def isInvalidNotification(notification: InternalNotification, tc: TransactionalContext): Boolean =
    notification match {
      case x: MissingLabelNotification =>
        tc.kernelTransaction().tokenRead().nodeLabel(x.label) != TokenRead.NO_TOKEN
      case x: MissingRelTypeNotification =>
        tc.kernelTransaction().tokenRead().relationshipType(x.relType) != TokenRead.NO_TOKEN
      case x: MissingPropertyNameNotification =>
        tc.kernelTransaction().tokenRead().propertyKey(x.name) != TokenRead.NO_TOKEN
      case _ => false
    }

  /**
   * Recompile a query with expression code generation if needed. Otherwise return the cached value.
   */
  private def recompileOrGet(
    executingQuery: ExecutingQuery,
    cachedValue: CachedValue,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    queryKey: QUERY_KEY,
    metaData: String
  ): EXECUTABLE_QUERY = {
    tracer.cacheHit(queryKey, metaData)
    executingQueryTracer.cacheHit(executingQuery)
    val newCachedValue =
      if (!cachedValue.recompiledWithExpressionCodeGen) {
        compiler.maybeCompileWithExpressionCodeGen(cachedValue.numberOfHits) match {
          case Some(recompiledQuery) =>
            tracer.computeWithExpressionCodeGen(queryKey, metaData)
            val recompiled = new CachedValue(recompiledQuery, recompiledWithExpressionCodeGen = true)
            inner.put(queryKey, recompiled)
            recompiled
          case None => cachedValue
        }
      } else cachedValue

    newCachedValue.value
  }

  private def compileAndCache(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    metaData: String,
    hitCache: Boolean = false
  ): EXECUTABLE_QUERY = {
    val result = compileOrCompileWithExpressionCodeGenAndCache(
      executingQuery,
      queryKey,
      () => compiler.compile(),
      metaData,
      hitCache
    )
    tracer.compute(queryKey, metaData)
    result
  }

  private def compileWithExpressionCodeGenAndCache(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    metaData: String,
    hitCache: Boolean = false
  ): EXECUTABLE_QUERY = {
    val result = compileOrCompileWithExpressionCodeGenAndCache(
      executingQuery,
      queryKey,
      () => compiler.compileWithExpressionCodeGen(),
      metaData,
      hitCache
    )
    tracer.computeWithExpressionCodeGen(queryKey, metaData)
    result
  }

  /**
   * Ensure this query is recompiled and put it in the cache.
   *
   * Compilation is either done in this thread, or by some other thread if it got there
   * first. Regardless of who does it, this is treated as a cache miss, because it will
   * take a long time. The only exception is if hitCache is true, which should only happen
   * when we are forced to recompile due to previously present warnings not being valid anymore
   */
  private def compileOrCompileWithExpressionCodeGenAndCache(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    compile: () => EXECUTABLE_QUERY,
    metaData: String,
    hitCache: Boolean
  ): EXECUTABLE_QUERY = {
    val newExecutableQuery = compile()
    if (newExecutableQuery.shouldBeCached) {
      val cachedValue = new CachedValue(newExecutableQuery, recompiledWithExpressionCodeGen = false)
      inner.put(queryKey, cachedValue)
      if (hitCache)
        hit(executingQuery, queryKey, cachedValue, metaData)
      else
        miss(executingQuery, queryKey, newExecutableQuery, metaData)
    } else {
      miss(executingQuery, queryKey, newExecutableQuery, metaData)
    }
  }

  private def hit(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    executableQuery: CachedValue,
    metaData: String
  ): EXECUTABLE_QUERY = {
    tracer.cacheHit(queryKey, metaData)
    executingQueryTracer.cacheHit(executingQuery)
    executableQuery.value
  }

  private def miss(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    newExecutableQuery: EXECUTABLE_QUERY,
    metaData: String
  ): EXECUTABLE_QUERY = {
    tracer.cacheMiss(queryKey, metaData)
    executingQueryTracer.cacheMiss(executingQuery)
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
    tracer.cacheFlush(priorSize)
    priorSize
  }
}

object QueryCache {

  final case class CacheKey[QUERY_REP](
    queryRep: QUERY_REP,
    parameterTypeMap: ParameterTypeMap,
    txStateHasChanges: Boolean
  )

  val NOT_PRESENT: ExecutableQuery = null

  /**
    * Representation of the query parameter types for a query invocation.
    *
    * This class receives a hashCode which is precomputed by [[extractParameterTypeMap()]], because it
    * is much faster to pre-compute the hash than to call `resultMap.hashCode()`.
    */
  class ParameterTypeMap private[QueryCache] (
    private val resultMap: java.util.Map[String, ParameterTypeInfo],
    _hashCode: Int
  ) {
    override def hashCode(): Int = _hashCode

    override def equals(obj: Any): Boolean = {
      obj match {
        case other: ParameterTypeMap if resultMap.size == other.resultMap.size =>
          val otherMap = other.resultMap
          val entries = otherMap.entrySet.iterator()
          var stillEqual = true

          while (entries.hasNext && stillEqual) {
            val entry = entries.next()
            val otherKey = entry.getKey
            val otherValue = entry.getValue
            val value = resultMap.get(otherKey)
            stillEqual = otherValue.equals(value)
          }

          stillEqual
        case _ =>
          false
      }
    }

    // Implemented to simplify testing
    override def toString: String = resultMap.asScala.toString
  }

  object ParameterTypeMap {
    final val empty = new ParameterTypeMap(new java.util.HashMap(), 0)
  }

  /**
   * Use this method to extract ParameterTypeMap from MapValue that represents parameters
   */
  def extractParameterTypeMap(mapValue: MapValue, useSizeHint: Boolean): ParameterTypeMap = {
    val resultMap = new java.util.HashMap[String, ParameterTypeInfo]
    var hashCode = 0
    mapValue.foreach((key, value) => {
      val valueType = ParameterValueTypeHelper.deriveCypherType(value, useSizeHint)
      resultMap.put(key, valueType)
      hashCode = hashCode ^ (key.hashCode + 31 * valueType.hashCode())
    })
    new ParameterTypeMap(resultMap, hashCode)
  }
}
