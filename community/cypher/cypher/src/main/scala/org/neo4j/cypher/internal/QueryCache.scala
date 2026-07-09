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
import org.neo4j.cypher.internal.QueryCache.CompileReason
import org.neo4j.cypher.internal.QueryCache.QueryCacheResult
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.MissingLabelNotification
import org.neo4j.cypher.internal.notification.MissingPropertyNameNotification
import org.neo4j.cypher.internal.notification.MissingRelTypeNotification
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.token.api.TokenConstants
import org.neo4j.values.virtual.MapValue

import java.io.Closeable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.NANOSECONDS

import scala.annotation.tailrec
import scala.concurrent.ExecutionException
import scala.concurrent.TimeoutException
import scala.jdk.CollectionConverters.IteratorHasAsScala
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
   * @param hitCount        the number of cache hits for that query
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
 * @param maximumSize     Maximum size of this cache
 * @param stalenessCaller Decided whether CachedExecutionPlans are stale
 * @param tracer          Traces cache activity
 */
class QueryCache[QUERY_KEY <: AnyRef, EXECUTABLE_QUERY <: CacheabilityInfo](
  val cacheFactory: CaffeineCacheFactory,
  val maximumSize: CacheSize,
  val stalenessCaller: PlanStalenessCaller[EXECUTABLE_QUERY],
  val tracer: CacheTracer[QUERY_KEY],
  val executingQueryTracer: ExecutingQueryTracer
) extends Closeable {

  val removalListener: RemovalListener[QUERY_KEY, CacheEntry] =
    (key: QUERY_KEY, _: CacheEntry, _: RemovalCause) => tracer.discard(key, "")

  private val inner: Cache[QUERY_KEY, CacheEntry] =
    createInner(cacheFactory, maximumSize, removalListener)

  def close(): Unit = inner match {
    case closable: java.io.Closeable => closable.close()
    case _                           => ()
  }

  protected def createInner(
    innerFactory: CaffeineCacheFactory,
    size: CacheSize,
    listener: RemovalListener[QUERY_KEY, CacheEntry]
  ): Cache[QUERY_KEY, CacheEntry] = {
    innerFactory.createCache[QUERY_KEY, CacheEntry](size, listener)
  }

  def estimatedSize(): Long = inner.estimatedSize()

  // Warning! Expensive and can have effect on cache eviction because all values are accessed.
  def values: Iterator[CachedValue] = inner.asMap().values().iterator().asScala.collect { case v: CachedValue => v }

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
     * //Rethrow any exception that was encountered while performing the computation.
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
          case _: ExecutionException =>
            // In case of an exception, each waiting Thread will have to do the computation themselves.
            // This is to avoid for example that an exception from a transaction with a short timeout
            // is propagated to a transaction with a long timeout that could have finished.
            return DoItYourself
        }
      }
      throw new IllegalStateException("cannot get here")
    }

    /**
     * Mark the computation as done with the provided computed value.
     * This wakes up all Threads that were wating on the computation.
     *
     * @param computedValue the computed value
     */
    def done(computedValue: ComputationTarget): Unit = future.complete(computedValue)

    /**
     * Mark the computation as failed with the provided exception.
     * This wakes up all Threads that were wating on the computation.
     *
     * @param e the thrown exception.
     */
    def failed(e: Throwable): Unit = future.completeExceptionally(e)
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
   * Retrieves a cached execution plan (and associated metadata) for a given query key. If the query is not found
   * in the cache or is considered stale, it will be re-compiled and updated
   * in the cache. This method ensures proper invalidation and recomputation strategies based on the
   * provided replan option.
   *
   * @param queryKey       the query key to retrieve the execution plan for.
   * @param tc             the transactional context in which the computation is executed
   * @param compiler       the compiler used to compile or recompile the query
   * @param replanStrategy the strategy indicating whether to force recompilation, skip it, or use defaults
   * @param metaData       optional metadata associated with the query for tracing or diagnostic purposes
   * @param cacheStrategy  the caching strategy determining if and how values should be cached
   * @return a QueryCacheResult containing the executable query (the execution plan) and associated metadata, such as compile reason and waiting time for further reporting.
   */
  final def computeIfAbsentOrStale(
    queryKey: QUERY_KEY,
    tc: TransactionalContext,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    replanStrategy: CypherReplanOption,
    metaData: String = "",
    cacheStrategy: CacheStrategy = CacheStrategy.defaultDefault
  ): QueryCacheResult[EXECUTABLE_QUERY] =
    recursivelyComputeIfAbsentOrStale(
      queryKey,
      tc,
      compiler,
      replanStrategy,
      metaData,
      cacheStrategy,
      accumulatedWaitTimeMillis = 0L
    )

  @tailrec
  final private def recursivelyComputeIfAbsentOrStale(
    queryKey: QUERY_KEY,
    tc: TransactionalContext,
    compiler: CompilerWithExpressionCodeGenOption[EXECUTABLE_QUERY],
    replanStrategy: CypherReplanOption,
    metaData: String,
    cacheStrategy: CacheStrategy,
    accumulatedWaitTimeMillis: Long
  ): QueryCacheResult[EXECUTABLE_QUERY] = {

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
     *
     * @param cachedValue the cached value
     * @return A [[QueryCacheResult[EXECUTABLE_QUERY]]] that contains the executable query along with metadata.
     *         Or [[None]], if checking the cache should be retried.
     */
    def processCachedValue(cachedValue: CachedValue): Option[QueryCacheResult[EXECUTABLE_QUERY]] = {
      // mark as seen from cache
      cachedValue.markHit()

      replanStrategy match {
        case CypherReplanOption.force =>
          Some(QueryCacheResult(
            compileCodeGen(
              hitCache = true,
              // When forcibly re-planning, do not use a BeingPlanned to let Threads without `replan=force` use the cached value.
              beingComputed = None
            ),
            Some(CompileReason.UserForcedReplan),
            accumulatedWaitTimeMillis
          ))
        case CypherReplanOption.skip =>
          Some(QueryCacheResult(
            hit(executingQuery, queryKey, cachedValue, metaData),
            None,
            accumulatedWaitTimeMillis
          ))
        case CypherReplanOption.default =>
          stalenessCaller.staleness(tc, cachedValue.value) match {
            case NotStale =>
              if (invalidNotificationExisting(cachedValue, tc)) {
                val beingRecomputed = new BeingRecomputed(cachedValue)
                if (!inner.asMap().replace(queryKey, cachedValue, beingRecomputed)) {
                  // Some other Thread already replaced the value. Retry.
                  None
                } else {
                  tracer.logText(
                    "Cached query plan is not used due to existing notifications that are not valid anymore",
                    metaData
                  )
                  Some(QueryCacheResult(
                    compile(hitCache = true, Some(beingRecomputed)),
                    Some(CompileReason.StaleNotifications),
                    accumulatedWaitTimeMillis
                  ))
                }
              } else {
                recompileOrGet(executingQuery, cachedValue, compiler, queryKey, metaData, accumulatedWaitTimeMillis)
              }
            case Stale(secondsSincePlan, maybeReason) =>
              val beingRecomputed = new BeingRecomputed(cachedValue)
              if (!inner.asMap().replace(queryKey, cachedValue, beingRecomputed)) {
                // Some other Thread already replaced the value. Retry.
                None
              } else {
                tracer.cacheStale(queryKey, secondsSincePlan, metaData, maybeReason)
                Some(QueryCacheResult(
                  compileIfNeededWithCodeGen(
                    codeGen = cachedValue.recompiledWithExpressionCodeGen,
                    hitCache = true,
                    Some(beingRecomputed)
                  ),
                  Some(CompileReason.StaleStatistics),
                  accumulatedWaitTimeMillis
                ))
              }
          }
      }
    }

    lazy val executingQuery = tc.executingQuery()
    if (!shouldBeCached(cacheStrategy)) {
      val result = compiler.compile()
      // NOTE: We assume queryKey is unused by tracer.compute here, as we do not have a queryKey when shouldBeCached(cacheStrategy) = false
      tracer.compute(queryKey, result.codeGenByteCodeSize, metaData)
      QueryCacheResult(result, Some(CompileReason.SkipCache), accumulatedWaitTimeMillis)
    } else if (maximumSize.currentValue == 0) {
      val result = compiler.compile()
      tracer.compute(queryKey, result.codeGenByteCodeSize, metaData)
      QueryCacheResult(result, Some(CompileReason.CacheSize0), accumulatedWaitTimeMillis)
    } else {
      // Mark as being computed if not present
      val beingComputed = new BeingComputed()
      inner.get(queryKey, _ => beingComputed) match {
        case `beingComputed` =>
          // If this is the beingComputed that we just inserted into the cache:
          QueryCacheResult(
            compileIfNeededWithCodeGen(
              codeGen = replanStrategy == CypherReplanOption.force,
              hitCache = false,
              Some(beingComputed)
            ),
            Some(CompileReason.CacheMiss),
            accumulatedWaitTimeMillis
          )

        case beingRecomputed: BeingRecomputed if replanStrategy == CypherReplanOption.skip =>
          // Just return the old value
          QueryCacheResult(
            hit(executingQuery, queryKey, beingRecomputed.oldValue, metaData),
            None,
            accumulatedWaitTimeMillis
          )

        case _: BeingComputed if replanStrategy == CypherReplanOption.force =>
          // Even if there is an ongoing computation, we should replan (concurrently)
          QueryCacheResult(
            compileCodeGen(hitCache = true, None),
            Some(CompileReason.UserForcedReplan),
            accumulatedWaitTimeMillis
          )

        case beingComputed: BeingComputed =>
          val waitStart = System.nanoTime()
          tracer.awaitOngoingComputation(queryKey, metaData)
          // Wait until the other Thread is done
          beingComputed.await(tc.kernelTransaction()) match {
            case cachedValue: CachedValue =>
              val waitTimeMillis = NANOSECONDS.toMillis(System.nanoTime() - waitStart)
              // The duplicated code is on purpose not pulled into processCachedValue,
              // to enable the tail-recursive call.
              processCachedValue(cachedValue) match {
                case Some(queryCacheResult) =>
                  queryCacheResult.copy(waitTimeMillis = accumulatedWaitTimeMillis + waitTimeMillis)
                case None =>
                  // Retry
                  recursivelyComputeIfAbsentOrStale(
                    queryKey,
                    tc,
                    compiler,
                    replanStrategy,
                    metaData,
                    cacheStrategy,
                    accumulatedWaitTimeMillis + waitTimeMillis
                  )
              }
            case DoItYourself =>
              val waitTimeMillis = NANOSECONDS.toMillis(System.nanoTime() - waitStart)
              // We must perform the computation ourselves.
              // Some computed values cannot be shared, e.g. for
              // AdministrationCommands with sensitive literals.
              // It is a bit unfortunate that we still had to wait until the other computation was done,
              // but generally one can only determine if a value can be shared after having computed the value.
              QueryCacheResult(
                compileIfNeededWithCodeGen(
                  codeGen = replanStrategy == CypherReplanOption.force,
                  hitCache = false,
                  None
                ),
                Some(CompileReason.CacheMiss),
                accumulatedWaitTimeMillis + waitTimeMillis
              )
          }

        case cachedValue: CachedValue =>
          processCachedValue(cachedValue) match {
            case Some(queryCacheResult) => queryCacheResult
            case None                   =>
              // Retry
              recursivelyComputeIfAbsentOrStale(
                queryKey,
                tc,
                compiler,
                replanStrategy,
                metaData,
                cacheStrategy,
                accumulatedWaitTimeMillis
              )
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
        tc.kernelTransaction().tokenRead().nodeLabel(x.label) != TokenConstants.NO_TOKEN
      case x: MissingRelTypeNotification =>
        tc.kernelTransaction().tokenRead().relationshipType(x.relType) != TokenConstants.NO_TOKEN
      case x: MissingPropertyNameNotification =>
        tc.kernelTransaction().tokenRead().propertyKey(x.name) != TokenConstants.NO_TOKEN
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
    metaData: String,
    accumulatedWaitTimeMillis: Long
  ): Option[QueryCacheResult[EXECUTABLE_QUERY]] = {
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
            tracer.logText(
              "Cached query plan is not used because recompilation with expression code generation is triggered",
              metaData
            )
            tracer.computeWithExpressionCodeGen(queryKey, recompiledQuery.codeGenByteCodeSize, metaData)
            val recompiled = new CachedValue(recompiledQuery, recompiledWithExpressionCodeGen = true)
            inner.put(queryKey, recompiled)
            // If we get here, beingRecomputed must have been assigned.
            beingRecomputed.done(recompiled)
            Some(QueryCacheResult(
              recompiled.value,
              Some(CompileReason.RecompiledWithCodeGen),
              accumulatedWaitTimeMillis
            ))
          case None =>
            // We can end up here because of 2 reasons:
            // 1) The query was not yet executed often enough to trigger a recompilation.
            //    We want to return the old cached value in this case.
            // 2) This Thread lost the race to recompute the value. We want to retry and await the
            //    Thread performing the computation in this case.
            // We distinguish the cases by checking if beingRecomputed got assigned.
            if (beingRecomputed == null) {
              // Case 1)
              Some(QueryCacheResult(cachedValue.value, None, accumulatedWaitTimeMillis))
            } else {
              // Case 2)
              None
            }
        }
      } else Some(QueryCacheResult(cachedValue.value, None, accumulatedWaitTimeMillis))

      if (result.isDefined) {
        tracer.cacheHit(queryKey, metaData)
        executingQueryTracer.cacheHit(executingQuery)
      }

      result
    } catch {
      case e: Throwable =>
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
    tracer.compute(queryKey, result.codeGenByteCodeSize, metaData)
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
    tracer.computeWithExpressionCodeGen(queryKey, result.codeGenByteCodeSize, metaData)
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
      case e: Throwable if (beingComputed.isDefined) =>
        // In case there is a `beingComputed`, we need to complete it with the thrown exception,
        // to wake up other Threads.
        try {
          // We must not leave the beingComputed instance in the cache, otherwise the query can never succeed again on transient errors.
          inner.invalidate(queryKey)
          // Wake up Threads waiting for this computation.
          beingComputed.get.failed(e)
        } catch {
          case e2: Throwable =>
            // Another error occurred during cache invalidation
            if (e != e2) {
              e.addSuppressed(e2)
            }
        }
        throw e
    }
  }

  protected def shouldBeCached(cacheStrategy: CacheStrategy): Boolean = {
    cacheStrategy.unknownKindShouldBeCached
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
    txStateHasChanges: Boolean,
    resolvedLanguage: CypherVersion
  )

  val NOT_PRESENT: ExecutableQuery = null

  sealed trait CompileReason {
    def asText: String = this.toString
  }

  object CompileReason {
    // When the query is not in the cache, the planner will be invoked.
    case object CacheMiss extends CompileReason

    // The query was in the cache, but the statistics used to plan the query are stale.
    case object StaleStatistics extends CompileReason

    // The previous plan contained notifications like missing labels or properties that are not valid anymore.
    case object StaleNotifications extends CompileReason

    // The query was identified to be hit frequently enough to trigger a recompilation of sections of the execution plan to bytecode. The logical plan remains unchanged.
    case object RecompiledWithCodeGen extends CompileReason

    // The query was planned, and it was determined that the query should not be cached, e.g., there were debug options, incomplete parameters, etc.
    case object SkipCache extends CompileReason

    // A cache hit, but the user specified that the query should be re-planned.
    case object UserForcedReplan extends CompileReason

    // Same as a cache-miss, but the user has specified to disallow caching of ALL queries by setting the cache size to 0.
    case object CacheSize0 extends CompileReason
  }

  case class QueryCacheResult[T](
    executableQuery: T,
    compileReason: Option[CompileReason] = None,
    waitTimeMillis: Long
  )

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
