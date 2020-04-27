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

import org.mockito.Mockito
import org.mockito.Mockito.times
import org.mockito.Mockito.verifyNoMoreInteractions
import org.neo4j.cypher.CypherReplanOption
import org.neo4j.cypher.internal.QueryCache.ParameterTypeMap
import org.neo4j.cypher.internal.QueryCacheTest.TC
import org.neo4j.cypher.internal.QueryCacheTest.alwaysStale
import org.neo4j.cypher.internal.QueryCacheTest.compiled
import org.neo4j.cypher.internal.QueryCacheTest.compilerWithExpressionCodeGenOption
import org.neo4j.cypher.internal.QueryCacheTest.newCache
import org.neo4j.cypher.internal.QueryCacheTest.newKey
import org.neo4j.cypher.internal.QueryCacheTest.newTracer
import org.neo4j.cypher.internal.QueryCacheTest.staleAfterNTimes
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.helpers.collection.Pair
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import org.scalatest.mockito.MockitoSugar

class QueryCacheTest extends CypherFunSuite {

  test("size 0 cache should never 'hit' or 'miss' and never compile with expression code generation") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer, size = 0)
    val key = newKey("foo")

    // When
    val v1 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    v1 should equal(compiled(key))
    v1.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val v2 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    v2 should equal(compiled(key))
    v2.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("first time accessing the cache should be a cache miss") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("accessing the cache with two different keys should both result in cache misses") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key1 = newKey("key1")
    val key2 = newKey("key2")


    // When
    val value1FromCache = cache.computeIfAbsentOrStale(key1, TC, compilerWithExpressionCodeGenOption(key1), CypherReplanOption.default)
    // Then
    value1FromCache should equal(compiled(key1))
    value1FromCache.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key1, "")
    o.verify(tracer).queryCompile(key1, "")
    verifyNoMoreInteractions(tracer)

    // When
    val value2FromCache = cache.computeIfAbsentOrStale(key2, TC, compilerWithExpressionCodeGenOption(key2), CypherReplanOption.default)
    // Then
    value2FromCache should equal(compiled(key2))
    value2FromCache.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).queryCacheMiss(key2, "")
    o.verify(tracer).queryCompile(key2, "")
    verifyNoMoreInteractions(tracer)
  }

  test("second time accessing the cache should be a cache hit") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)
    o.verify(tracer).queryCacheHit(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("accessing the cache with replan=force should be a cache miss even if item exists and is not stale. It should also immediately compile with expression code generation.") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    val valueFromCache1 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.force)
    // Then
    valueFromCache1 should equal(compiled(key))
    valueFromCache1.compiledWithExpressionCodeGen should equal(true)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val valueFromCache2 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.force)
    // Then
    valueFromCache2 should equal(compiled(key))
    valueFromCache2.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("if item is stale we should miss the cache") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val cache = newCache(tracer, alwaysStale(secondsSinceReplan))
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    o.verify(tracer).queryCacheStale(key, secondsSinceReplan, "", None)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("accessing the cache with replan=skip if item is stale we should hit the cache") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val cache = newCache(tracer, alwaysStale(secondsSinceReplan))
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    o.verify(tracer).queryCacheHit(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("should trigger recompile when hot") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default)

    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(true)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    o.verify(tracer, times(3)).queryCacheHit(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("if item is stale but was compiled with expression code generation we should miss the cached and directly compile with expression code generation") {
    // Given
    val tracer = newTracer()
    val secondsSinceReplan = 17
    val cache = newCache(tracer, staleAfterNTimes(secondsSinceReplan, 3))
    val key = newKey("foo")

    // When
    val v1 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // miss, compile
    // Then
    v1 should equal(compiled(key))
    v1.compiledWithExpressionCodeGen should equal(false)
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // hit
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // hit
    val v2 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // hit, compile-exp-code-gen
    // Then
    v2 should equal(compiled(key))
    v2.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer, times(3)).queryCacheHit(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)

    // When
    val v3 = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default) // stale, miss, compile-exp-code-gen
    // Then
    v3 should equal(compiled(key))
    v3.compiledWithExpressionCodeGen should equal(true)
    o.verify(tracer).queryCacheStale(key, secondsSinceReplan, "", None)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("accessing the cache with replan=skip should not recompile hot queries") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)
    val valueFromCache = cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.skip)

    // Then
    valueFromCache should equal(compiled(key))
    valueFromCache.compiledWithExpressionCodeGen should equal(false)

    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    o.verify(tracer, times(3)).queryCacheHit(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("should only trigger recompile once") {
    // Given
    val tracer = newTracer()
    val cache = newCache(tracer)
    val key = newKey("foo")

    // When
    (1 to 100).foreach(_ => cache.computeIfAbsentOrStale(key, TC, compilerWithExpressionCodeGenOption(key), CypherReplanOption.default))

    // Then
    val o = Mockito.inOrder(tracer)
    o.verify(tracer).queryCacheMiss(key, "")
    o.verify(tracer).queryCompile(key, "")
    o.verify(tracer, times(3)).queryCacheHit(key, "")
    o.verify(tracer).queryCompileWithExpressionCodeGen(key, "")
    o.verify(tracer, times(96)).queryCacheHit(key, "")
    verifyNoMoreInteractions(tracer)
  }

  test("parameterTypeMap should equal if same parameters") {
    val params1 = VirtualValues.map(Array("a", "b", "c"), Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true))))
    val params2 = VirtualValues.map(Array("a", "b", "c"), Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true))))
    val typeMap1 = QueryCache.extractParameterTypeMap(params1)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2)
    typeMap1.hashCode() shouldBe typeMap2.hashCode()
    typeMap1 should equal(typeMap2)
    typeMap2 should equal(typeMap1)
  }

  test("parameterTypeMap should equal if same types but different values") {
    val params1 = VirtualValues.map(Array("a"), Array(Values.of("a")));
    val params2 = VirtualValues.map(Array("a"), Array(Values.of("b")));
    val typeMap1 = QueryCache.extractParameterTypeMap(params1)
    val typeMap2 = QueryCache.extractParameterTypeMap(params2)
    typeMap1.hashCode() shouldBe typeMap2.hashCode()
    typeMap1 should equal(typeMap2)
    typeMap2 should equal(typeMap1)
  }

  test("parameterTypeMap should not equal") {
    val params = Seq(
      VirtualValues.map(Array("a", "b", "c"), Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))),
      VirtualValues.map(Array("a", "b", "d"), Array(Values.of(3), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))),
      VirtualValues.map(Array("a", "b", "d"), Array(Values.of("ho"), Values.of("hi"), VirtualValues.list(Values.of(false), Values.of(true)))),
      VirtualValues.map(Array("a", "b", "d"), Array(Values.of("ho"), Values.of("hi"), VirtualValues.map(Array("key"), Array(Values.of(true))))),
      VirtualValues.map(Array("a", "b"),      Array(Values.of("ho"), Values.of("hi")))
    )

    for {
      p1 <- params
      p2 <- params
    } {
      if (p1 != p2) {
        withClue(s"    $p1\n != $p2\n") {
          val typeMap1 = QueryCache.extractParameterTypeMap(p1)
          val typeMap2 = QueryCache.extractParameterTypeMap(p2)
          typeMap1.hashCode() shouldNot be(typeMap2.hashCode())
          typeMap1 shouldNot equal(typeMap2)
        }
      }
    }
  }
}

object QueryCacheTest extends MockitoSugar {
  case class MyValue(key: String)(val compiledWithExpressionCodeGen: Boolean) extends CacheabilityInfo {
    override def shouldBeCached: Boolean = true

    override def notifications: IndexedSeq[InternalNotification] = IndexedSeq.empty
  }

  val TC: TransactionalContext = mock[TransactionalContext]
  type Tracer = CacheTracer[Pair[String, ParameterTypeMap]]
  type Key = Pair[String, ParameterTypeMap]

  private val RECOMPILE_LIMIT = 2
  def compilerWithExpressionCodeGenOption(key: Key): CompilerWithExpressionCodeGenOption[MyValue] = new CompilerWithExpressionCodeGenOption[MyValue] {
    override def compile(): MyValue = compiled(key)

    override def compileWithExpressionCodeGen(): MyValue = compiledWithExpressionCodeGen(key)

    override def maybeCompileWithExpressionCodeGen(hitCount: Int): Option[MyValue] =
      if (hitCount > RECOMPILE_LIMIT) Some(compiledWithExpressionCodeGen(key))
      else None
  }

  def newKey(string: String): Key = Pair.of(string, ParameterTypeMap.empty)

  def newCache(tracer: Tracer = newTracer(), stalenessCaller: PlanStalenessCaller[MyValue] = neverStale(), size: Int = 10): QueryCache[String, Pair[String, ParameterTypeMap], MyValue] = {
    new QueryCache[String, Pair[String, ParameterTypeMap], MyValue](size, stalenessCaller, tracer)
  }

  def newTracer(): Tracer = mock[Tracer]

  private def neverStale(): PlanStalenessCaller[MyValue] = (_, _) => NotStale

  private def alwaysStale(seconds: Int): PlanStalenessCaller[MyValue] = (_, _) => Stale(seconds, None)

  private def staleAfterNTimes(seconds: Int, n: Int): PlanStalenessCaller[MyValue] = new PlanStalenessCaller[MyValue] {
    private var invocations = 0
    override def staleness(transactionalContext: TransactionalContext,
                           cachedExecutableQuery: MyValue): Staleness = {
      invocations += 1
      if (invocations > n) Stale(seconds, None)
      else NotStale
    }
  }

  private def compiled(key: Key): MyValue = MyValue(key.first())(compiledWithExpressionCodeGen = false)
  private def compiledWithExpressionCodeGen(key: Key): MyValue = MyValue(key.first())(compiledWithExpressionCodeGen = true)
}
