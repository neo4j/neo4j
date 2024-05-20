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
import com.github.benmanes.caffeine.cache.RemovalListener
import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.internal.QueryCache.CacheKey
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.QueryCacheTest.MyValue
import org.neo4j.cypher.internal.QueryCacheTest.QueryCacheUsageQueue
import org.neo4j.cypher.internal.QueryCacheTest.TC
import org.neo4j.cypher.internal.QueryCacheTest.Tracer
import org.neo4j.cypher.internal.QueryCacheTest.alwaysStale
import org.neo4j.cypher.internal.QueryCacheTest.compiled
import org.neo4j.cypher.internal.QueryCacheTest.compilerWithExpressionCodeGenOption
import org.neo4j.cypher.internal.QueryCacheTest.neverStale
import org.neo4j.cypher.internal.QueryCacheTest.newKey
import org.neo4j.cypher.internal.QueryCacheTest.newTracer
import org.neo4j.cypher.internal.QueryCacheTest.staleAfterNTimes
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.options.CypherReplanOption
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.query.QueryCacheUsage
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable

class QueryCacheTest extends CypherFunSuite {

  def newCache(
    tracer: Tracer = newTracer(),
    stalenessCaller: PlanStalenessCaller[MyValue] = neverStale(),
    size: Int = 10,
    queryTracer: ExecutingQueryTracer = ExecutingQueryTracer.NoOp
  ): QueryCache[CacheKey[String], MyValue] = QueryCacheTest.newCache(tracer, stalenessCaller, size, queryTracer)

  test("size 0 cache should never 'hit' or 'miss' and never compile with expression code generation") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, size = 0, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    val v1 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    v1 should equal(compiled(key))
    v1.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val v2 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    v2 should equal(compiled(key))
    v2.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.queueIsEmmpty shouldBe true
  }

  test("first time accessing the cache should be a cache miss") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("accessing the cache with two different keys should both result in cache misses") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key1 = newKey("key1")
    val key2 = newKey("key2")

    // When
    val value1FromCache =
      cache.computeIfAbsentOrStale(key1, TC, compilerWithExpressionCodeGenOption(key1), CypherReplanOption.default)
    // Then
    value1FromCache should equal(compiled(key1))
    value1FromCache.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key1, "")
    o.verify(tracer).compute(key1, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val value2FromCache =
      cache.computeIfAbsentOrStale(key2, TC, compilerWithExpressionCodeGenOption(key2), CypherReplanOption.default)
    // Then
    value2FromCache should equal(compiled(key2))
    value2FromCache.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).cacheMiss(key2, "")
    o.verify(tracer).compute(key2, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("second time accessing the cache should be a cache hit") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.HIT
    queryTracer.queueIsEmmpty shouldBe true
  }

  test(
    "accessing the cache with replan=force should be a cache hit even if item exists and is not stale. It should also immediately compile with expression code generation."
  ) {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    val valueFromCache1 =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.force)
    // Then
    valueFromCache1 should equal(compiled(key))
    valueFromCache1.compiledWithExpressionCodeGen should equal(true)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val valueFromCache2 =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.force)
    // Then
    valueFromCache2 should equal(compiled(key))
    valueFromCache2.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer).cacheHit(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.HIT
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("if item is stale we should recalculate") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, alwaysStale(secondsSinceReplan), queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    o.verify(tracer).cacheStale(key, secondsSinceReplan, "", None)
    o.verify(tracer).cacheHit(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.HIT
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("accessing the cache with replan=skip if item is stale we should hit the cache") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, alwaysStale(secondsSinceReplan), queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    o.verify(tracer).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.HIT
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("should trigger recompile when hot") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)

    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(true)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    o.verify(tracer, times(2)).cacheHit(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    o.verify(tracer).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueAllCacheUsage() shouldEqual (QueryCacheUsage.MISS +: Seq.fill(3)(QueryCacheUsage.HIT))
  }

  test(
    "if item is stale but was compiled with expression code generation we should directly compile with expression code generation"
  ) {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, staleAfterNTimes(secondsSinceReplan, 3), queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    val v1 =
      cache.computeIfAbsentOrStale(
        key,
        TC,
        compilerWithExpressionCodeGenOption(key),
        CypherReplanOption.default
      ) // miss, compile
    // Then
    v1 should equal(compiled(key))
    v1.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.MISS
    queryTracer.queueIsEmmpty shouldBe true

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // hit
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // hit
    val v2 =
      cache.computeIfAbsentOrStale(
        key,
        TC,
        compilerWithExpressionCodeGenOption(key),
        CypherReplanOption.default
      ) // hit, compile-exp-code-gen
    // Then
    v2 should equal(compiled(key))
    v2.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer, times(2)).cacheHit(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    o.verify(tracer).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueAllCacheUsage() shouldEqual Seq.fill(3)(QueryCacheUsage.HIT)

    // When
    val v3 =
      cache.computeIfAbsentOrStale(
        key,
        TC,
        compilerWithExpressionCodeGenOption(key),
        CypherReplanOption.default
      ) // stale, miss, compile-exp-code-gen
    // Then
    v3 should equal(compiled(key))
    v3.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer).cacheStale(key, secondsSinceReplan, "", None)
    o.verify(tracer).cacheHit(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueCacheUsage() shouldEqual QueryCacheUsage.HIT
    queryTracer.queueIsEmmpty shouldBe true
  }

  test("accessing the cache with replan=skip should not recompile hot queries") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    val valueFromCache =
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)

    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    o.verify(tracer, times(3)).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueAllCacheUsage() shouldEqual (QueryCacheUsage.MISS +: Seq.fill(3)(QueryCacheUsage.HIT))
  }

  test("should only trigger recompile once") {
    // Given
    val tracer = newTracer()
    val queryTracer = new QueryCacheUsageQueue()
    val cache = newCache(tracer, queryTracer = queryTracer)
    val key = newKey("foo")

    // When
    (1 to 100).foreach(_ =>
      cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    )

    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).cacheMiss(key, "")
    o.verify(tracer).compute(key, "")
    o.verify(tracer, times(2)).cacheHit(key, "")
    o.verify(tracer).computeWithExpressionCodeGen(key, "")
    o.verify(tracer, times(97)).cacheHit(key, "")
    verifyNoMoreInteractions(tracer)

    queryTracer.dequeueAllCacheUsage() shouldEqual (QueryCacheUsage.MISS +: Seq.fill(99)(QueryCacheUsage.HIT))
  }

  test("parameterTypeMap should equal if same parameters") {
    val params1 = VirtualValues.map(
      Array("a", "b", "c"),
      Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))
    )
    val params2 = VirtualValues.map(
      Array("a", "b", "c"),
      Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))
    )
    val typeMap1 = QueryCache.extractParameterTypeMap(params1, useSizeHint = false)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2, useSizeHint = false)
    typeMap1.hashCode() shouldBe typeMap2.hashCode()
    typeMap1 should equal(typeMap2)
    typeMap2 should equal(typeMap1)
  }

  test("parameterTypeMap should equal if same types but different values") {
    val params1 = VirtualValues.map(Array("a"), Array(Values.of("a")))
    val params2 = VirtualValues.map(Array("a"), Array(Values.of("b")))
    val typeMap1 = QueryCache.extractParameterTypeMap(params1, useSizeHint = false)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2, useSizeHint = false)
    typeMap1.hashCode() shouldBe typeMap2.hashCode()
    typeMap1 should equal(typeMap2)
    typeMap2 should equal(typeMap1)
  }

  test("parameterTypeMap should not equal if same types but very different length of strings (if using size hint)") {
    val params1 = VirtualValues.map(Array("a"), Array(Values.of("a")))
    val params2 = VirtualValues.map(Array("a"), Array(Values.of("b".repeat(1000))))
    val typeMap1 = QueryCache.extractParameterTypeMap(params1, useSizeHint = true)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2, useSizeHint = true)
    typeMap1 shouldNot equal(typeMap2)
    typeMap2 shouldNot equal(typeMap1)
  }

  test("parameterTypeMap should not equal if same types but very different length of lists (if using size hint)") {
    val params1 = VirtualValues.map(Array("a"), Array(VirtualValues.range(0, 1, 1)))
    val params2 = VirtualValues.map(Array("a"), Array(VirtualValues.range(0, 1001, 1)))
    val typeMap1 = QueryCache.extractParameterTypeMap(params1, useSizeHint = true)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2, useSizeHint = true)
    typeMap1 shouldNot equal(typeMap2)
    typeMap2 shouldNot equal(typeMap1)
  }

  test("parameterTypeMap should not equal") {
    val params = Seq(
      VirtualValues.map(
        Array("a", "b", "c"),
        Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))
      ),
      VirtualValues.map(
        Array("a", "b", "d"),
        Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))
      ),
      VirtualValues.map(
        Array("a", "b", "d"),
        Array(Values.of("ho"), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))
      ),
      VirtualValues.map(
        Array("a", "b", "d"),
        Array(Values.of("ho"), Values.of("hi"), VirtualValues.map(Array("key"), Array(Values.of(true))))
      ),
      VirtualValues.map(Array("a", "b"), Array(Values.of("ho"), Values.of("hi")))
    )

    for {
      p1 <- params
      p2 <- params
    } {
      if (p1 != p2) {
        withClue(s"    $p1\n != $p2\n") {
          val typeMap1 = QueryCache.extractParameterTypeMap(p1, useSizeHint = false)
          val typeMap2 = QueryCache.extractParameterTypeMap(p2, useSizeHint = false)
          typeMap1 shouldNot equal(typeMap2)
        }
      }
    }
  }

}

class SoftQueryCacheTest extends QueryCacheTest {

  override def newCache(
    tracer: Tracer,
    stalenessCaller: PlanStalenessCaller[MyValue],
    size: Int,
    queryTracer: ExecutingQueryTracer
  ): QueryCache[CacheKey[String], MyValue] = QueryCacheTest.newSoftCache(tracer, stalenessCaller, size, queryTracer)
}

object QueryCacheTest extends MockitoSugar {

  case class MyValue(key: String)(val compiledWithExpressionCodeGen: Boolean) extends CacheabilityInfo {
    override def shouldBeCached: Boolean = true

    override def notifications: IndexedSeq[InternalNotification] = IndexedSeq.empty
  }

  val TC: TransactionalContext = mock[TransactionalContext]
  private val RECOMPILE_LIMIT = 2
  private val cacheFactory = TestExecutorCaffeineCacheFactory

  type Key = CacheKey[String]
  type Tracer = CacheTracer[Key]

  def compilerWithExpressionCodeGenOption(key: Key): CompilerWithExpressionCodeGenOption[MyValue] =
    new CompilerWithExpressionCodeGenOption[MyValue] {
      override def compile(): MyValue = compiled(key)

      override def compileWithExpressionCodeGen(): MyValue = compiledWithExpressionCodeGen(key)

      override def maybeCompileWithExpressionCodeGen(hitCount: Int, shouldRecompile: () => Boolean): Option[MyValue] =
        if (hitCount > RECOMPILE_LIMIT && shouldRecompile()) {
          Some(compiledWithExpressionCodeGen(key))
        } else None
    }

  def newKey(string: String): Key = CacheKey(string, ParameterTypeMap.empty, txStateHasChanges = false)

  def newCache(
    tracer: Tracer = newTracer(),
    stalenessCaller: PlanStalenessCaller[MyValue] = neverStale(),
    size: Int = 10,
    queryTracer: ExecutingQueryTracer = ExecutingQueryTracer.NoOp
  ): QueryCache[CacheKey[String], MyValue] = {
    new QueryCache[CacheKey[String], MyValue](
      cacheFactory,
      CacheSize.Static(size),
      stalenessCaller,
      tracer,
      queryTracer
    )
  }

  def newSoftCache(
    tracer: Tracer = newTracer(),
    stalenessCaller: PlanStalenessCaller[MyValue] = neverStale(),
    size: Int = 10,
    queryTracer: ExecutingQueryTracer = ExecutingQueryTracer.NoOp,
    softSize: Int = 10
  ): QueryCache[CacheKey[String], MyValue] = {
    new QueryCache[CacheKey[String], MyValue](
      cacheFactory,
      CacheSize.Static(size),
      stalenessCaller,
      tracer,
      queryTracer
    ) {
      override protected def createInner(
        innerFactory: CaffeineCacheFactory,
        size: CacheSize,
        listener: RemovalListener[CacheKey[String], CacheEntry]
      ): Cache[CacheKey[String], CacheEntry] = {
        innerFactory.createWithSoftBackingCache(size, CacheSize.Static(softSize), listener)
      }
    }
  }

  def newTracer(): Tracer = mock[Tracer]

  private def neverStale(): PlanStalenessCaller[MyValue] = (_, _) => NotStale

  private def alwaysStale(seconds: Int): PlanStalenessCaller[MyValue] = (_, _) => Stale(seconds, None)

  private def staleAfterNTimes(seconds: Int, n: Int): PlanStalenessCaller[MyValue] = new PlanStalenessCaller[MyValue] {
    private var invocations = 0

    override def staleness(transactionalContext: TransactionalContext, cachedExecutableQuery: MyValue): Staleness = {
      invocations += 1
      if (invocations > n) Stale(seconds, None)
      else NotStale
    }
  }

  private def compiled(key: Key): MyValue = MyValue(key.queryRep)(compiledWithExpressionCodeGen = false)

  private def compiledWithExpressionCodeGen(key: Key): MyValue =
    MyValue(key.queryRep)(compiledWithExpressionCodeGen = true)

  class QueryCacheUsageQueue() extends ExecutingQueryTracer {
    private val cacheUsage = new mutable.Queue[QueryCacheUsage]()

    def dequeueCacheUsage(): QueryCacheUsage = cacheUsage.dequeue()

    def dequeueAllCacheUsage(): Seq[QueryCacheUsage] = cacheUsage.dequeueAll(_ => true)

    def queueIsEmmpty: Boolean = cacheUsage.isEmpty

    override def cacheHit(executingQuery: ExecutingQuery): Unit = cacheUsage.enqueue(QueryCacheUsage.HIT)

    override def cacheMiss(executingQuery: ExecutingQuery): Unit = cacheUsage.enqueue(QueryCacheUsage.MISS)
  }
}
