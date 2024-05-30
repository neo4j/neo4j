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
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.internal.kernel.api.TokenRead
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.notifications.MissingLabelNotification
import org.neo4j.notifications.MissingPropertyNameNotification
import org.neo4j.notifications.MissingRelTypeNotification
import org.neo4j.values.virtual.MapValue

import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.concurrent.ExecutionException
import scala.concurrent.TimeoutException
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
   * Decide whether a previously compiled query should be using expression code generation now,
   * and do that in that case.
   *
   * @param hitCount the number of cache hits for that query
   * @param shouldRecompile A callback to decide whether this Thread should recompile.
   *                        This callback checks if some other Thread concurrently performs the same computation.
   *                        If so, this Thread is paused, and, after the computation of the other Thread is done,
   *                        the callback returns `false`. Otherwise, this callback returns `true`.
   *                        <p>
   *                        This callback _must_ be invoked last, if there are several checks.
   *                        In other words: If the callback is invoked and returns true,
   *                        then expression compilation _must_ be performed.
   *                        This callback _must_ only be invoked once.
   * @return `Some(compiled-query-with-expression-code-gen)` if expression code generation was deemed useful,
   *         `None` otherwise.
   */
  def maybeCompileWithExpressionCodeGen(hitCount: Int, shouldRecompile: () => Boolean): Option[EXECUTABLE_QUERY]
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

  val removalListener: RemovalListener[QUERY_KEY, CacheEntry] =
    (key: QUERY_KEY, value: CacheEntry, cause: RemovalCause) => tracer.discard(key, "")

  private val inner: Cache[QUERY_KEY, CacheEntry] =
    createInner(cacheFactory, maximumSize, removalListener)

  protected def createInner(
    innerFactory: CaffeineCacheFactory,
    size: CacheSize,
    listener: RemovalListener[QUERY_KEY, CacheEntry]
  ): Cache[QUERY_KEY, CacheEntry] = {
    innerFactory.createCache[QUERY_KEY, CacheEntry](size, listener)
  }

  def estimatedSize(): Long = inner.estimatedSize()

  /**
   * An entry in this cache. Can either be an actual value, or a placeholder for a value currently being computed.
   */
  sealed trait CacheEntry

  /**
   * A value being computed.
   */
  sealed trait ComputationTarget

  /**
   * A placeholder for a value currently being computed.
   * Other threads can register themselves to be woken up when the computation is done.
   * When the computation is done, the placeholder will be replaced with an actual [[CachedValue]].
   */
  class BeingComputed() extends CacheEntry {
    private val future: CompletableFuture[ComputationTarget] = new CompletableFuture()

    /**
     * Block this Thread until the computation is done. Return the computed value.
     * Rethrow any exception that was encountered while performing the computation.
     */
    def await(assertOpen: AssertOpen): ComputationTarget = {
      // Use an endless loop and a time-limit to test if TX is still alive, periodically.
      while (true) {
        try {
          return future.get(100, TimeUnit.MILLISECONDS)
        } catch {
          case _: TimeoutException =>
            // Check that tx is still open, then retry
            assertOpen.assertOpen()
          case ee: ExecutionException =>
            throw ee.getCause
        }
      }
      throw new IllegalStateException("cannot get here")
    }

    /**
     * Mark the computation as done with the provided computed value.
     * This wakes up all Threads that were wating on the computation.
     * @param computedValue the computed value
     */
    def done(computedValue: ComputationTarget): Unit = future.complete(computedValue)

    /**
     * Mark the computation as failed with the provided exception.
     * This wakes up all Threads that were wating on the computation.
     * @param e the thrown exception.
     */
    def failed(e: Exception): Unit = future.completeExceptionally(e)
  }

  /**
   * A placeholder for a value that is already in the cache, but is currently being recomputed.
   * This can happen due to stale values, or expression compilation.
   *
   * @param oldValue the value that was previously in the cache.
   */
  class BeingRecomputed(val oldValue: CachedValue)
      extends BeingComputed()

  /**
   * The cached value wraps the value and maintains a count of how many times it has been fetched from the cache
   * and whether or not it has been recompiled with expression code generation.
   */
  class CachedValue(val value: EXECUTABLE_QUERY, val recompiledWithExpressionCodeGen: Boolean) extends CacheEntry
      with ComputationTarget {

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
   * A computed value that indicates that other Threads _must_ recompute the value,
   * because the values cannot be shared between Threads.
   */
  case object DoItYourself extends ComputationTarget

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
  @tailrec
  final def computeIfAbsentOrStale(
    queryKey: QUERY_KEY,
    tc: TransactionalContext,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    replanStrategy: CypherReplanOption,
    metaData: String = ""
  ): EXECUTABLE_QUERY = {

    def compile(hitCache: Boolean, beingComputed: Option[BeingComputed]): EXECUTABLE_QUERY =
      compileAndCache(
        executingQuery,
        queryKey,
        compiler,
        metaData,
        hitCache = hitCache,
        beingComputed
      )

    def compileCodeGen(hitCache: Boolean, beingComputed: Option[BeingComputed]): EXECUTABLE_QUERY =
      compileWithExpressionCodeGenAndCache(
        executingQuery,
        queryKey,
        compiler,
        metaData,
        hitCache = hitCache,
        beingComputed = beingComputed
      )

    def compileIfNeededWithCodeGen(
      codeGen: Boolean,
      hitCache: Boolean,
      beingComputed: Option[BeingComputed]
    ): EXECUTABLE_QUERY =
      if (codeGen)
        compileCodeGen(hitCache = hitCache, beingComputed)
      else
        compile(hitCache = hitCache, beingComputed)

    /**
      * Process a value that is already in the cache.
      * @param cachedValue the cached value
      * @return An [[EXECUTABLE_QUERY]] that can be returned from [[computeIfAbsentOrStale()]].
      *         Or [[None]], if checking the cache should be retried.
      */
    def processCachedValue(cachedValue: CachedValue): Option[EXECUTABLE_QUERY] = {
      // mark as seen from cache
      cachedValue.markHit()

      replanStrategy match {
        case CypherReplanOption.force =>
          // When forcibly re-planning, do not use a BeingPlanned to let Threads without `replan=force` use the cached value.
          Some(compileCodeGen(hitCache = true, None))
        case CypherReplanOption.skip =>
          Some(hit(executingQuery, queryKey, cachedValue, metaData))
        case CypherReplanOption.default =>
          stalenessCaller.staleness(tc, cachedValue.value) match {
            case NotStale =>
              if (invalidNotificationExisting(cachedValue, tc)) {
                val beingRecomputed = new BeingRecomputed(cachedValue)
                if (!inner.asMap().replace(queryKey, cachedValue, beingRecomputed)) {
                  // Some other Thread already replaced the value. Retry.
                  None
                } else {
                  Some(compile(hitCache = true, Some(beingRecomputed)))
                }
              } else {
                recompileOrGet(executingQuery, cachedValue, compiler, queryKey, metaData)
              }
            case Stale(secondsSincePlan, maybeReason) =>
              val beingRecomputed = new BeingRecomputed(cachedValue)
              if (!inner.asMap().replace(queryKey, cachedValue, beingRecomputed)) {
                // Some other Thread already replaced the value. Retry.
                None
              } else {
                tracer.cacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
                Some(compileIfNeededWithCodeGen(
                  codeGen = cachedValue.recompiledWithExpressionCodeGen,
                  hitCache = true,
                  Some(beingRecomputed)
                ))
              }
          }
      }
    }

    lazy val executingQuery = tc.executingQuery()
    if (maximumSize.currentValue == 0) {
      val result = compiler.compile()
      tracer.compute(queryKey, metaData)
      result
    } else {
      // Mark as being computed if not present
      val beingComputed = new BeingComputed()
      inner.get(queryKey, _ => beingComputed) match {
        case `beingComputed` =>
          // If this is the beingComputed that we just inserted into the cache:
          compileIfNeededWithCodeGen(
            codeGen = replanStrategy == CypherReplanOption.force,
            hitCache = false,
            Some(beingComputed)
          )

        case beingRecomputed: BeingRecomputed if replanStrategy == CypherReplanOption.skip =>
          // Just return the old value
          hit(executingQuery, queryKey, beingRecomputed.oldValue, metaData)

        case _: BeingComputed if replanStrategy == CypherReplanOption.force =>
          // Even if there is an ongoing computation, we should replan (concurrently)
          compileCodeGen(hitCache = true, None)

        case beingComputed: BeingComputed =>
          tracer.awaitOngoingComputation(queryKey, metaData)
          // Wait until the other Thread is done
          beingComputed.await(tc.kernelTransaction()) match {
            case cachedValue: CachedValue =>
              // The duplicated code is on purpose not pulled into processCachedValue,
              // to enable the tail-recursive call.
              processCachedValue(cachedValue) match {
                case Some(returnValue) => returnValue
                case None              =>
                  // Retry
                  computeIfAbsentOrStale(queryKey, tc, compiler, replanStrategy, metaData)
              }
            case DoItYourself =>
              // We must perform the computation ourselves.
              // Some computed values cannot be shared, e.g. for
              // AdministrationCommands with sensitive literals.
              // It is a bit unfortunate that we still had to wait until the other computation was done,
              // but generally one can only determine if a value can be shared after having computed the value.
              compileIfNeededWithCodeGen(
                codeGen = replanStrategy == CypherReplanOption.force,
                hitCache = false,
                None
              )
          }

        case cachedValue: CachedValue =>
          processCachedValue(cachedValue) match {
            case Some(returnValue) => returnValue
            case None              =>
              // Retry
              computeIfAbsentOrStale(queryKey, tc, compiler, replanStrategy, metaData)
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
  ): Option[EXECUTABLE_QUERY] = {
    var beingRecomputed: BeingRecomputed = null
    def onRecompilation(): Boolean = {
      // This will try to replace the current value with a BeingRecomputed.
      // This only succeeds if the value in the cache has not changed since.
      // We forward the return value of replace here. That means:
      // If replacing succeeded, the Thread will go ahead and recompute.
      // If replacing did not succeed, the Thread will retry.
      beingRecomputed = new BeingRecomputed(cachedValue)
      inner.asMap().replace(queryKey, cachedValue, beingRecomputed)
    }

    try {
      val result = if (!cachedValue.recompiledWithExpressionCodeGen) {
        compiler.maybeCompileWithExpressionCodeGen(cachedValue.numberOfHits, onRecompilation _) match {
          case Some(recompiledQuery) =>
            tracer.computeWithExpressionCodeGen(queryKey, metaData)
            val recompiled = new CachedValue(recompiledQuery, recompiledWithExpressionCodeGen = true)
            inner.put(queryKey, recompiled)
            // If we get here, beingRecomputed must have been assigned.
            beingRecomputed.done(recompiled)
            Some(recompiled.value)
          case None =>
            // We can end up here because of 2 reasons:
            // 1) The query was not yet executed often enough to trigger a recompilation.
            //    We want to return the old cached value in this case.
            // 2) This Thread lost the race to recompute the value. We want to retry and await the
            //    Thread performing the computation in this case.
            // We distinguish the cases by checking if beingRecomputed got assigned.
            if (beingRecomputed == null) {
              // Case 1)
              Some(cachedValue.value)
            } else {
              // Case 2)
              None
            }
        }
      } else Some(cachedValue.value)

      if (result.isDefined) {
        tracer.cacheHit(queryKey, metaData)
        executingQueryTracer.cacheHit(executingQuery)
      }

      result
    } catch {
      case e: Exception =>
        // In case there is a `beingRecomputed`, we need to complete it with the thrown exception,
        // to wake up other Threads.
        if (beingRecomputed != null) {
          // We must not leave the beingComputed instance in the cache, otherwise the query can never succeed again on transient errors.
          inner.invalidate(queryKey)
          beingRecomputed.failed(e)
        }
        throw e
    }
  }

  private def compileAndCache(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    metaData: String,
    hitCache: Boolean,
    beingComputed: Option[BeingComputed]
  ): EXECUTABLE_QUERY = {
    val result = compileOrCompileWithExpressionCodeGenAndCache(
      executingQuery,
      queryKey,
      () => compiler.compile(),
      metaData,
      hitCache,
      recompiledWithExpressionCodeGen = false,
      beingComputed
    )
    tracer.compute(queryKey, metaData)
    result
  }

  private def compileWithExpressionCodeGenAndCache(
    executingQuery: ExecutingQuery,
    queryKey: QUERY_KEY,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    metaData: String,
    hitCache: Boolean,
    beingComputed: Option[BeingComputed]
  ): EXECUTABLE_QUERY = {
    val result = compileOrCompileWithExpressionCodeGenAndCache(
      executingQuery,
      queryKey,
      () => compiler.compileWithExpressionCodeGen(),
      metaData,
      hitCache,
      recompiledWithExpressionCodeGen = true,
      beingComputed
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
    hitCache: Boolean,
    recompiledWithExpressionCodeGen: Boolean,
    beingComputed: Option[BeingComputed]
  ): EXECUTABLE_QUERY = {
    try {
      val newExecutableQuery = compile()
      if (newExecutableQuery.shouldBeCached) {
        val cachedValue =
          new CachedValue(newExecutableQuery, recompiledWithExpressionCodeGen = recompiledWithExpressionCodeGen)
        inner.put(queryKey, cachedValue)
        beingComputed.foreach(_.done(cachedValue))
        if (hitCache)
          hit(executingQuery, queryKey, cachedValue, metaData)
        else
          miss(executingQuery, queryKey, newExecutableQuery, metaData)
      } else {
        // We should not leave the beingComputed instance in the cache, otherwise we will get a wrong count of awaits when the next Thread comes.
        inner.invalidate(queryKey)
        // Other Threads must recompute a value that should not be cached.
        beingComputed.foreach(_.done(DoItYourself))
        miss(executingQuery, queryKey, newExecutableQuery, metaData)
      }
    } catch {
      case e: Exception =>
        // In case there is a `beingComputed`, we need to complete it with the thrown exception,
        // to wake up other Threads.
        beingComputed.foreach { bc =>
          // We must not leave the beingComputed instance in the cache, otherwise the query can never succeed again on transient errors.
          inner.invalidate(queryKey)
          // Wake up Threads waiting for this computation.
          bc.failed(e)
        }
        throw e
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
